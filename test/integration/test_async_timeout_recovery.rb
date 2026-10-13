# frozen_string_literal: true

require_relative '../helper'
require 'fileutils'
require 'rbconfig'
require 'async' if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.3')

# Replies from a separate process, so they arrive while a fiber in this process holds the thread.
# A key named "delay<ms>-..." delays its reply; every get writes one byte to #signal on receipt.
class DelayedReplyServer
  SCRIPT = <<~'RUBY'
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
            value = "value-of-#{key}"
            connection.write("VA #{value.bytesize}#{" k#{key}" if flags.include?('k')}\r\n#{value}\r\n")
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

module AsyncTimeoutRecoveryHelpers
  RECOVER = { exp_async_timeout_recovery: true }.freeze
  SOCKET_TIMEOUT = 0.05
  BUSY_DURATION = 0.15
  WAKE_PATHS = %i[io_readiness ready_queue timer].freeze
  SELECTORS =
    if defined?(IO::Event::Selector)
      %i[URing EPoll KQueue Select].select { |name| IO::Event::Selector.const_defined?(name) }.freeze
    else
      [].freeze
    end

  def recover
    RECOVER
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

module RecoveryParityHelpers
  MULTI_KEYS = (Array.new(40) { |i| "multi-#{i}" } << 'missing').freeze

  def store_fixtures(client)
    client.flush
    client.set('small', 'value')
    client.set('large', 'L' * 700_000)
    # Individual sets: a 40 x 3KB set_multi over TLS can stall memcached, and Ruby < 3.4 never times it out.
    MULTI_KEYS.take(40).each { |key| client.set(key, "#{key}-#{'m' * 3_000}", 0) }
  end

  def read_results(client, req)
    [
      client.get('small', req), client.get('missing', req), client.get('large', req),
      client.get_multi(*MULTI_KEYS, **req),
      client.get_multi_with_status('small', 'missing', **req).transform_values(&:value),
      client.get_with_status('small', req).value, client.get_cas('small', req).first,
      client.gat('small', 60, req), client.touch('small', 60, req)
    ]
  end

  def write_results(client, req)
    [
      client.fetch('fetched', nil, req) { 'computed' },
      client.incr('counter', 2, nil, 10, req), client.incr('counter', 2, nil, 10, req),
      client.set('small', 'changed', nil, req).is_a?(Integer), client.get('small', req),
      client.delete('small', req), client.get('small', req)
    ]
  end

  def assert_same_results(client)
    store_fixtures(client)
    expected = read_results(client, {}) + write_results(client, {})
    store_fixtures(client)
    sock = connection_manager(client).sock

    assert_equal expected, read_results(client, recover) + write_results(client, recover)
    assert_same sock, connection_manager(client).sock
  end
end

describe 'exp_async_timeout_recovery' do
  include AsyncTimeoutRecoveryHelpers
  include RecoveryParityHelpers

  describe 'without a fiber scheduler' do
    it 'returns the same results as default reads over TCP' do
      memcached_persistent(21_345, '', { raw: true, compress: false }) { |client| assert_same_results(client) }
    end

    it 'returns the same results as default reads over a UNIX socket' do
      memcached_persistent(MemcachedManager::UNIX_SOCKET_PATH, '', { raw: true, compress: false }) do |client|
        assert_same_results(client)
      end
    end

    it 'returns the same results as default reads over SSL' do
      memcached_ssl_persistent do |_, port|
        client = Dalli::Client.new("localhost:#{port}", ssl_context: CertificateGenerator.ssl_context,
                                                        raw: true, compress: false)

        assert_same_results(client)
      ensure
        client&.close
      end
    end

    it 'shares a connection with default reads' do
      memcached_persistent(21_345, '', { raw: true, compress: false }) do |client|
        store_fixtures(client)
        sock = connection_manager(client).sock
        20.times do |i|
          req = i.even? ? recover : {}

          assert_equal 'L' * 700_000, client.get('large', req)
          assert_equal 40, client.get_multi(*RecoveryParityHelpers::MULTI_KEYS, **req).size
        end

        assert_same sock, connection_manager(client).sock
      end
    end

    it 'does not allocate a reader for requests that do not opt in' do
      memcached_persistent(21_345, '', { raw: true, compress: false }) do |client|
        store_fixtures(client)
        read_results(client, {})
        write_results(client, {})

        assert_nil connection_manager(client).instance_variable_get(:@timeout_recovery_reader)
      end
    end

    it 'still fails a read when no reply arrives before the timeout' do
      with_delayed_reply_client do |client|
        client.alive!
        duration = elapsed do
          assert_error(Dalli::NetworkError, /IO::TimeoutError/) { client.get('delay300ms', recover) }
        end

        assert_operator duration, :<, 0.3
        assert_equal 'value-of-b', client.get('b', recover)
      end
    end
  end

  AsyncTimeoutRecoveryHelpers::SELECTORS.each do |selector|
    describe "under Async with the #{selector} selector" do
      AsyncTimeoutRecoveryHelpers::WAKE_PATHS.each do |wake|
        it "returns a reply that arrives while a fiber woken by #{wake} holds the thread" do
          with_delayed_reply_client do |client, server|
            value = sock = nil
            duration = with_selector(selector) do |task|
              client.alive!
              sock = connection_manager(client).sock
              start_busy_fiber(task, wake, server.signal)
              elapsed { value = client.get('delay20ms-a', recover) }
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
            client.get_multi('delay20ms-a', 'b', **recover)
          end

          assert_equal({ 'delay20ms-a' => 'value-of-delay20ms-a', 'b' => 'value-of-b' }, values)
        end
      end

      it 'fails without the option when a fiber woken by io_readiness holds the thread' do
        with_delayed_reply_client do |client, server|
          error = with_selector(selector) do |task|
            client.alive!
            start_busy_fiber(task, :io_readiness, server.signal)
            client.get('delay20ms-a')
            nil
          rescue Dalli::NetworkError => e
            e
          end

          assert_kind_of Dalli::NetworkError, error
          assert_match(/IO::TimeoutError/, error.message)
        end
      end

      it 'still fails a read when no reply arrives before the timeout' do
        with_delayed_reply_client do |client|
          error = nil
          duration = with_selector(selector) do
            client.alive!
            elapsed do
              client.get('delay300ms', recover)
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
            reader = task.async { client.get('delay500ms', recover) }
            sleep(0.01)
            reader.stop
            reader.wait
            manager = connection_manager(client)
            [manager.connected?, manager.request_in_progress?, client.get('b', recover)]
          end

          assert_equal [false, false, 'value-of-b'], state
        end
      end

      it 'returns the same results as default reads' do
        memcached_persistent(21_345, '', { raw: true, compress: false }) do |client|
          with_selector(selector) { assert_same_results(client) }
        end
      end
    end
  end
end
