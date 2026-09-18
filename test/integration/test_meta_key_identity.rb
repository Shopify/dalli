# typed: true
# frozen_string_literal: true

require_relative '../helper'

require 'timeout'

describe('single-key meta response identity') do
  def serve_key_response(socket, response_key, requests)
    while (line = socket.gets("\r\n"))
      case line
      when "version\r\n"
        socket.write("VERSION 1.6.39-fake\r\n")
      when /\Amg /
        requests << line
        socket.write("EN k#{response_key}\r\n")
      else
        socket.write("ERROR\r\n")
      end
    end
  ensure
    socket.close
  end

  def with_key_echo_server(response_keys)
    tcp_server = TCPServer.new('127.0.0.1', 0)
    requests = Queue.new
    server_thread = Thread.new do
      response_keys.each do |response_key|
        serve_key_response(tcp_server.accept, response_key, requests)
      end
    rescue IOError, Errno::EBADF
      nil
    end
    server_thread.abort_on_exception = true

    yield(tcp_server.addr[1], requests)
  ensure
    tcp_server&.close
    server_thread&.kill
    server_thread&.join
  end

  {
    'fast raw get' => [{ raw: true }, "mg expected v k\r\n"],
    'formatted get' => [{}, "mg expected v f k\r\n"]
  }.each do |name, (options, expected_request)|
    it("validates and disconnects after a mismatch on the #{name} path") do
      with_key_echo_server(%w[different expected]) do |port, requests|
        client = Dalli::Client.new("127.0.0.1:#{port}", options)
        begin
          error = assert_raises(Dalli::ResponseKeyMismatchError) do
            client.get('expected')
          end

          assert_equal('Response key did not match request', error.message)
          assert_nil(client.get('expected'))
          assert_equal(expected_request, Timeout.timeout(2) { requests.pop })
          assert_equal(expected_request, Timeout.timeout(2) { requests.pop })
        ensure
          client.close
        end
      end
    end
  end
end
