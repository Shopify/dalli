# frozen_string_literal: true

require 'minitest/autorun'
require 'socket'
require 'tmpdir'
require 'timeout'
require 'dalli'

# Exercise the public client against complete wire batches. No client internals
# are stubbed, including the reconnect after an unread failed batch.
class MultiGetErrorServer
  attr_reader :address, :connections

  def initialize(replies)
    @directory = Dir.mktmpdir('dalli-multi-errors-')
    @address = File.join(@directory, 'cache.sock')
    @listener = UNIXServer.new(@address)
    @replies = replies.dup
    @connections = Queue.new
    @thread = Thread.new { run }
  end

  def close
    @thread.kill
    @thread.value
  ensure
    @listener.close
    File.unlink(@address)
    Dir.rmdir(@directory)
  end

  private

  def run
    connection_id = 0
    loop do
      connection = @listener.accept
      connection_id += 1
      serve(connection, connection_id)
    end
  end

  def serve(connection, connection_id)
    while (line = connection.gets("\r\n"))
      case line.split.first
      when 'version' then connection.write("VERSION 1.6.41-test\r\n")
      when 'mg' then next
      when 'mn'
        @connections << connection_id
        connection.write(@replies.shift || raise('Unexpected extra batch'))
      else raise "Unexpected request: #{line.inspect}"
      end
    end
  rescue Errno::ECONNRESET, Errno::EPIPE
    # The client closes a failed batch with unread bytes still on the socket.
    nil
  ensure
    connection.close
  end
end

describe 'single-server meta multi-get error propagation' do
  def with_batches(replies, raw:)
    server = MultiGetErrorServer.new(replies)
    client = Dalli::Client.new(server.address, protocol: :meta, raw: raw,
                                               socket_timeout: 0.5, socket_failure_delay: 0)
    Timeout.timeout(3) { yield client, server }
  ensure
    client&.close
    server&.close
  end

  [true, false].each do |raw|
    it "preserves hits and misses with raw=#{raw}" do
      with_batches(["VA 1 f0 khit\r\n3\r\nEN\r\nMN\r\n", "MN\r\n"], raw: raw) do |client, server|
        assert_equal({ 'hit' => '3' }, client.get_multi('hit', 'missing'))
        assert_empty client.get_multi('absent', 'missing')
        assert_equal server.connections.pop, server.connections.pop
      end
    end

    {
      'SERVER_ERROR upstream error' => Dalli::ServerError,
      'CLIENT_ERROR bad command line format' => Dalli::DalliError,
      'ERROR' => Dalli::DalliError
    }.each do |error_line, error_type|
      {
        'all errors' => "#{error_line}\r\n#{error_line}\r\nMN\r\n",
        'hit then error' => "VA 1 f0 khit\r\n3\r\n#{error_line}\r\nMN\r\n",
        'error then hit' => "#{error_line}\r\nVA 1 f0 khit\r\n3\r\nMN\r\n"
      }.each do |layout, failed_batch|
        it "raises on #{layout} (#{error_line}, raw=#{raw}) and reconnects without leftover data" do
          with_batches([failed_batch, "VA 1 f0 krecovered\r\n9\r\nMN\r\n"], raw: raw) do |client, server|
            error = assert_raises(error_type) { client.get_multi('hit', 'failed') }
            assert_includes error.message, error_line
            assert_equal({ 'recovered' => '9' }, client.get_multi('recovered', 'missing'))
            refute_equal server.connections.pop, server.connections.pop
          end
        end
      end
    end
  end
end
