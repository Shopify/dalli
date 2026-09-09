# frozen_string_literal: true

require_relative '../helper'

# A real byte stream allows rejected and valid frames to arrive together, or a
# rejected header to arrive without its body. Each connection is served until
# Dalli closes it, so subsequent requests expose accidental socket reuse.
class OpaqueCorrelationServer
  def initialize(&get_response)
    @listener = TCPServer.new('127.0.0.1', 0)
    @requests = Queue.new
    @get_response = get_response
    @thread = Thread.new { run }
  end

  def address
    "127.0.0.1:#{@listener.addr[1]}"
  end

  def requests
    Array.new(@requests.size) { @requests.pop }
  end

  def close
    @thread.kill
    @thread.value
  ensure
    @listener.close
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
      command = line.split.first
      @requests << [connection_id, command] unless command == 'version'
      response = case command
                 when 'version' then "VERSION 1.6.41-fake\r\n"
                 when 'mg' then @get_response.call(line)
                 when 'ma' then "VA 1\r\n2\r\n"
                 else raise "Unexpected command: #{line.inspect}"
                 end
      connection.write(response)
    end
  rescue Errno::ECONNRESET, Errno::EPIPE
    # Closing with unread response bytes may reset the peer's connection.
    nil
  ensure
    connection.close
  end
end

describe 'single-get opaque correlation' do
  def with_opaque_server(response)
    server = OpaqueCorrelationServer.new(&response)
    client = Dalli::Client.new(server.address, raw: true, socket_timeout: 0.5)
    yield client, server
  ensure
    client&.close
    server&.close
  end

  def assert_disconnected(client)
    manager = client.send(:ring).servers.first.instance_variable_get(:@connection_manager)

    refute_predicate manager, :connected?
    refute_predicate manager, :request_in_progress?
  end

  it 'does not let a queued get response become the next arithmetic result' do
    response = lambda do |line|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      "VA 5 Oobsolete\r\nstale\r\nVA 3 #{opaque}\r\n999\r\n"
    end

    with_opaque_server(response) do |client, server|
      assert_nil client.get('wanted')
      assert_disconnected(client)
      assert_equal 2, client.incr('counter')
      assert_equal [[1, 'mg'], [2, 'ma']], server.requests,
                   'the mismatch must not retry, and the next operation must use a fresh connection'
    end
  end

  it 'discards a rejected header without waiting for its declared body' do
    response = ->(_line) { "VA 1048576 Oobsolete\r\n" }

    with_opaque_server(response) do |client, server|
      Timeout.timeout(2) do
        assert_nil client.get('wanted')
        assert_disconnected(client)
        assert_equal 2, client.incr('counter')
      end
      assert_equal [[1, 'mg'], [2, 'ma']], server.requests
    end
  end

  opaque_flags = [' Oobsolete', '']
  [
    [:get, ['wanted'], nil],
    [:get, ['wanted', { cache_nils: true }], Dalli::NOT_FOUND],
    [:get, ['wanted', { meta_flags: ['t'] }], [nil, {}]],
    [:gat, ['wanted', 30], nil],
    [:gat, ['wanted', 30, { meta_flags: ['t'] }], [nil, {}]],
    [:get_cas, ['wanted'], [nil, 0]],
    [:get_with_status, ['wanted'], nil],
    [:touch, ['wanted', 30], nil]
  ].each do |operation, args, expected|
    opaque_flags.each do |opaque_flag|
      it "returns the normal miss and disconnects for #{operation}(#{args.inspect}) with #{opaque_flag.inspect}" do
        response = lambda do |_line|
          operation == :touch ? "HD#{opaque_flag}\r\n" : "VA 4 f1#{opaque_flag}\r\nNOPE\r\n"
        end

        with_opaque_server(response) do |client, server|
          result = client.public_send(operation, *args)
          if operation == :get_with_status
            assert_predicate result, :miss?
          elsif expected.nil?
            assert_nil result
          else
            assert_equal expected, result
          end

          assert_disconnected(client)
          assert_equal 2, client.incr('counter')
          assert_equal [[1, 'mg'], [2, 'ma']], server.requests
        end
      end
    end
  end

  it 'reconnects for the next get and reuses that connection for matching responses' do
    response = lambda do |line|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      if line.split[1] == 'bad'
        "EN Oobsolete\r\nVA 3 #{opaque}\r\n999\r\n"
      else
        "VA 5 f0 #{opaque}\r\nvalue\r\n"
      end
    end

    with_opaque_server(response) do |client, server|
      assert_nil client.get('bad')
      assert_disconnected(client)
      assert_equal 'value', client.get('good')
      assert_equal 'value', client.get('good')
      assert_equal [[1, 'mg'], [2, 'mg'], [2, 'mg']], server.requests
    end
  end
end
