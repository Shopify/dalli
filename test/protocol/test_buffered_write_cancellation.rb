# typed: true
# frozen_string_literal: true

require 'minitest/autorun'
require 'async' if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.3')
require 'dalli'
require 'fileutils'
require 'socket'
require 'English'

describe(Dalli::Protocol::ConnectionManager) do
  it('does not retransmit buffered bytes when a write is cancelled') do
    skip('native async dependencies require Ruby 3.3+') if Gem::Version.new(RUBY_VERSION) < Gem::Version.new('3.3')

    begin
      FileUtils.mkdir_p('tmp')
      path = "tmp/buffered-close-#{Process.pid}.sock"
      listener = UNIXServer.new(path)
      manager = Dalli::Protocol::ConnectionManager.new(path, nil, :unix, {})
      manager.establish_connection
      sender = manager.sock
      receiver = listener.accept
      padding = 'p' * 8192
      filled = 0
      loop do
        count = sender.write_nonblock(padding, exception: false)
        break if count == :wait_writable

        filled += count
      end
      payload = 'one write only!'
      collector = writer = nil

      Sync do |parent|
        writer = parent.async do
          manager.write(payload)
          manager.flush
        ensure
          manager.close
        end
        buffer = IO::Buffer.new(filled)
        count =
          if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('4.1')
            buffer.read(receiver, 0, filled)
          else
            buffer.read(receiver, filled)
          end

        assert_equal(filled, count)
        assert_equal('p' * filled, buffer.get_string)
        collector = Thread.new { receiver.read }
        writer.stop
      ensure
        receiver.close if $ERROR_INFO && !receiver.closed?
      end

      assert_predicate(writer, :stopped?)
      assert_operator(payload, :start_with?, collector.value)
    ensure
      receiver&.close unless receiver&.closed?
      manager&.close
      listener&.close
      FileUtils.rm_f(path) if path
      collector&.kill if collector&.alive?
      collector&.join
    end
  end

  it('keeps the parent connection usable when a fork closes its inherited copy') do
    skip('fork is not available') unless Process.respond_to?(:fork)

    begin
      listener = TCPServer.new('127.0.0.1', 0)
      manager = Dalli::Protocol::ConnectionManager.new('127.0.0.1', listener.addr[1], :tcp, {})
      manager.establish_connection
      receiver = listener.accept
      child = Process.fork do
        status = 1
        begin
          manager.close
          status = 0
        ensure
          exit!(status)
        end
      end
      _, status = Process.wait2(child)

      assert_predicate(status, :success?)
      manager.write('parent connection')
      manager.flush

      assert_equal('parent connection', receiver.read(17))
    ensure
      receiver&.close
      manager&.close
      listener&.close
    end
  end
end
