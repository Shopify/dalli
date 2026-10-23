# frozen_string_literal: true

require_relative '../helper'
require 'fileutils'
require 'rbconfig'
require 'async' if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.3')

# Replies from a separate process, so they arrive while a fiber in this process holds the thread.
# A key named "delay<ms>-..." delays its reply; every get writes one byte to #signal on receipt.
# A key containing "size<n>" gets an n-byte value; with "parts<n>" and "gap<ms>" the value is
# written in n parts with a pause before each later part. Halfway through each pause, once the
# client has drained the previous part and is waiting, a byte is written to #signal.
class DelayedReplyServer
  VALUE_OF = <<~'RUBY'
    def value_of(key)
      size = key[/size(\d+)/, 1]&.to_i
      return "value-of-#{key}" unless size

      pattern = "EN\r\nVA 3 k\r\n\0\xFF".b
      (pattern * ((size / pattern.bytesize) + 1)).byteslice(0, size)
    end
  RUBY
  singleton_class.class_eval(VALUE_OF)

  SCRIPT = VALUE_OF + <<~'RUBY'
    require 'socket'
    listener = UNIXServer.new(ARGV.fetch(0))
    signal = IO.for_fd(3, autoclose: false)
    signal.sync = true
    signal.write('R')
    loop do
      Thread.new(listener.accept) do |connection|
        while (line = connection.gets("\r\n"))
          command, key, *flags = line.split
          case command
          when 'version' then connection.write("VERSION 1.6.41-fake\r\n")
          when 'mn' then connection.write("MN\r\n")
          when 'mg'
            signal.write('.')
            sleep(key[/\Adelay(\d+)ms/, 1].to_i / 1000.0)
            value = value_of(key)
            parts = key[/parts(\d+)/, 1]&.to_i || 1
            part_size = (value.bytesize + parts - 1) / parts
            first, *rest = Array.new(parts) { |i| value.byteslice(i * part_size, part_size) }
            connection.write("VA #{value.bytesize}#{" k#{key}" if flags.include?('k')}\r\n", first,
                             rest.empty? ? "\r\n" : '')
            pause = key[/gap(\d+)ms/, 1].to_i / 2000.0
            rest.each_with_index do |part, index|
              sleep(pause)
              signal.write('.')
              sleep(pause)
              connection.write(part, index == rest.size - 1 ? "\r\n" : '')
            end
          else raise "Unexpected command: #{command}"
          end
        end
      rescue Errno::ECONNRESET, Errno::EPIPE
        nil
      ensure
        connection.close
      end
    end
  RUBY

  attr_reader :path, :signal

  def initialize
    FileUtils.mkdir_p('tmp')
    @path = File.expand_path("tmp/delayed-reply-#{Process.pid}-#{SecureRandom.hex(4)}.sock")
    @signal, signal_writer = IO.pipe
    @pid = Process.spawn(RbConfig.ruby, '-e', SCRIPT, @path, 3 => signal_writer)
    signal_writer.close
    raise 'delayed reply server did not start' unless @signal.wait_readable(10) && @signal.read(1) == 'R'
  end

  def close
    Process.kill('TERM', @pid)
    Process.wait(@pid)
  ensure
    @signal.close
    FileUtils.rm_f(@path)
  end
end

module TimeoutRecoveryHelpers
  SOCKET_TIMEOUT = 0.05
  BUSY_DURATION = 0.15
  WAKE_PATHS = %i[io_readiness ready_queue timer].freeze
  SELECTORS =
    if defined?(IO::Event::Selector)
      %i[URing EPoll KQueue Select].select { |name| IO::Event::Selector.const_defined?(name) }.freeze
    else
      [].freeze
    end

  def socket_timeout
    SOCKET_TIMEOUT
  end

  def with_delayed_reply_client
    server = DelayedReplyServer.new
    client = Dalli::Client.new(server.path, raw: true, socket_timeout: SOCKET_TIMEOUT, socket_max_failures: 1,
                                            down_retry_delay: 0, socket_failure_delay: false)
    Timeout.timeout(10) { yield client, server }
  ensure
    client&.close
    server&.close
  end

  def with_selector(name)
    reactor = Async::Reactor.new(selector: IO::Event::Selector.const_get(name).new(Fiber.current))
    reactor.run(finished: false) do |task|
      yield task
    ensure
      task.children&.each(&:stop)
    end.wait
  ensure
    Fiber.set_scheduler(nil)
  end

  def busy
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + BUSY_DURATION
    nil while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
  end

  def await_signal(signal)
    signal.wait_readable
    signal.read_nonblock(1)
  end

  def start_busy_fiber(task, wake, signal)
    case wake
    when :io_readiness
      task.async do
        await_signal(signal)
        busy
      end
    when :ready_queue
      queue = Thread::Queue.new
      task.async do
        await_signal(signal)
        queue.push(true)
      end
      task.async do
        queue.pop
        busy
      end
    when :timer
      task.async do
        sleep(0.005)
        busy
      end
    end
  end

  def connection_manager(client)
    client.send(:ring).servers.first.instance_variable_get(:@connection_manager)
  end

  def elapsed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  end
end

module LargeValueHelpers
  # More than a UNIX socket buffer holds, so the server's write of a first half returns only once the
  # client is reading inside the value.
  LARGE_SIZE = 900_000

  ##
  # Records reads and waits on a connection's socket.
  ##
  class SocketLog
    def self.now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def initialize(sock)
      events = @events = []
      sock.singleton_class.prepend(Module.new do
        define_method(:read_nonblock) do |*args, **kwargs|
          result = super(*args, **kwargs)
          events << [:read, result.is_a?(String) ? result.bytesize : 0, SocketLog.now]
          result
        end

        define_method(:wait_readable) do |timeout = nil|
          result = super(timeout)
          events << [:wait, timeout, result ? true : false]
          result
        end
      end)
    end

    def last_data_at
      @events.reverse_each.find { |kind, bytes| kind == :read && bytes.positive? }&.last
    end

    # Bytes received before each wait that reported a timeout after which the reader still found
    # data: through the poll that follows or, when that poll misses it, the read after the poll.
    def recoveries(timeout)
      received = 0
      @events.each_cons(3).filter_map do |event, poll, after|
        received += event[1] if event.first == :read
        received if event == [:wait, timeout, false] && found_data?(poll, after)
      end
    end

    private

    def found_data?(poll, after)
      poll == [:wait, 0, true] || (poll == [:wait, 0, false] && after.first == :read && after[1].positive?)
    end
  end

  def in_scheduler(selector, &)
    return yield unless selector

    with_selector(selector, &)
  end

  def assert_reads_through_pauses(client, selector)
    key = "size#{LARGE_SIZE}-parts6-gap30ms"
    value = nil
    duration = in_scheduler(selector) do
      client.alive!
      elapsed { value = client.get(key) }
    end

    assert_equal DelayedReplyServer.value_of(key), value
    # Five 30ms pauses: none outlasts the timeout, together they do.
    assert_operator duration, :>, 2 * socket_timeout
  end

  def assert_fails_within_timeout_of_last_byte(client, selector)
    error = hang = nil
    in_scheduler(selector) do
      client.alive!
      log = SocketLog.new(connection_manager(client).sock)
      client.get("size#{LARGE_SIZE}-parts2-gap500ms")
    rescue Dalli::NetworkError => e
      error = e
      hang = SocketLog.now - log.last_data_at
    end

    assert_kind_of Dalli::NetworkError, error
    assert_match(/IO::TimeoutError/, error.message)
    assert_operator hang, :>=, socket_timeout
    assert_operator hang, :<, socket_timeout + 0.1
    key = "size#{LARGE_SIZE}-after"

    assert_equal DelayedReplyServer.value_of(key), in_scheduler(selector) { client.get(key) }
  end

  # A fiber that wakes on the server's second signal, sent while the reader waits inside the value
  # for its second half, and holds the thread until after that half arrives.
  def start_busy_fiber_inside_value(task, signal)
    task.async do
      2.times { await_signal(signal) }
      busy
    end
  end
end

describe 'read timeout recovery' do
  include TimeoutRecoveryHelpers
  include LargeValueHelpers

  describe 'without a fiber scheduler' do
    it 'reads small, large, and multi-get values over SSL on one connection' do
      memcached_ssl_persistent do |_, port|
        client = Dalli::Client.new("localhost:#{port}", ssl_context: CertificateGenerator.ssl_context,
                                                        raw: true, compress: false)
        multi = Array.new(40) { |i| ["multi-#{i}", "multi-#{i}-#{'m' * 3_000}"] }.to_h
        client.set('small', 'value')
        client.set('large', 'L' * 700_000)
        # Individual sets: a 40 x 3KB set_multi over TLS can stall memcached, and Ruby < 3.4 never times it out.
        multi.each { |key, value| client.set(key, value) }
        sock = connection_manager(client).sock

        2.times do
          assert_equal 'value', client.get('small')
          assert_nil client.get('missing')
          assert_equal 'L' * 700_000, client.get('large')
          assert_equal multi, client.get_multi(*multi.keys, 'missing')
        end
        assert_same sock, connection_manager(client).sock
      ensure
        client&.close
      end
    end

    it 'still fails a read when no reply arrives before the timeout' do
      with_delayed_reply_client do |client|
        client.alive!
        duration = elapsed do
          assert_error(Dalli::NetworkError, /IO::TimeoutError/) { client.get('delay300ms') }
        end

        assert_operator duration, :<, 0.3
        assert_equal 'value-of-b', client.get('b')
      end
    end

    it 'reads a large value delivered in pauses that are each shorter than the timeout' do
      with_delayed_reply_client { |client| assert_reads_through_pauses(client, nil) }
    end

    it 'fails a large value within one timeout once it stops arriving' do
      with_delayed_reply_client { |client| assert_fails_within_timeout_of_last_byte(client, nil) }
    end
  end

  TimeoutRecoveryHelpers::SELECTORS.each do |selector|
    describe "under Async with the #{selector} selector" do
      TimeoutRecoveryHelpers::WAKE_PATHS.each do |wake|
        it "returns a reply that arrives while a fiber woken by #{wake} holds the thread" do
          with_delayed_reply_client do |client, server|
            value = sock = nil
            duration = with_selector(selector) do |task|
              client.alive!
              sock = connection_manager(client).sock
              start_busy_fiber(task, wake, server.signal)
              elapsed { value = client.get('delay20ms-a') }
            end

            assert_equal 'value-of-delay20ms-a', value
            assert_operator duration, :>, socket_timeout
            assert_same sock, connection_manager(client).sock
          end
        end
      end

      it 'returns multi-get replies that arrive while another fiber holds the thread' do
        with_delayed_reply_client do |client, server|
          values = with_selector(selector) do |task|
            client.alive!
            start_busy_fiber(task, :io_readiness, server.signal)
            client.get_multi('delay20ms-a', 'b')
          end

          assert_equal({ 'delay20ms-a' => 'value-of-delay20ms-a', 'b' => 'value-of-b' }, values)
        end
      end

      it 'keeps reading a large value whose rest arrives while another fiber holds the thread' do
        with_delayed_reply_client do |client, server|
          key = "size#{LargeValueHelpers::LARGE_SIZE}-parts2-gap40ms"
          value = sock = log = nil
          duration = with_selector(selector) do |task|
            client.alive!
            sock = connection_manager(client).sock
            log = LargeValueHelpers::SocketLog.new(sock)
            start_busy_fiber_inside_value(task, server.signal)
            elapsed { value = client.get(key) }
          end
          recoveries = log.recoveries(socket_timeout)

          assert_equal DelayedReplyServer.value_of(key), value
          assert_operator duration, :>, socket_timeout
          assert_same sock, connection_manager(client).sock
          assert recoveries.any? { |received| received.between?(100, LargeValueHelpers::LARGE_SIZE) },
                 "expected a wait inside the value to time out and find data; recovered after #{recoveries} bytes"
        end
      end

      it 'reads a large value delivered in pauses that are each shorter than the timeout' do
        with_delayed_reply_client { |client| assert_reads_through_pauses(client, selector) }
      end

      it 'fails a large value within one timeout once it stops arriving' do
        with_delayed_reply_client { |client| assert_fails_within_timeout_of_last_byte(client, selector) }
      end

      it 'still fails a read when no reply arrives before the timeout' do
        with_delayed_reply_client do |client|
          error = nil
          duration = with_selector(selector) do
            client.alive!
            elapsed do
              client.get('delay300ms')
            rescue Dalli::NetworkError => e
              error = e
            end
          end

          assert_kind_of Dalli::NetworkError, error
          assert_operator duration, :<, 0.3
        end
      end

      it 'closes the connection when the waiting fiber is cancelled' do
        with_delayed_reply_client do |client|
          state = with_selector(selector) do |task|
            client.alive!
            reader = task.async { client.get('delay500ms') }
            sleep(0.01)
            reader.stop
            reader.wait
            manager = connection_manager(client)
            [manager.connected?, manager.request_in_progress?, client.get('b')]
          end

          assert_equal [false, false, 'value-of-b'], state
        end
      end
    end
  end
end
