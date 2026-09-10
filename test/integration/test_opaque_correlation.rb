# frozen_string_literal: true

require_relative '../helper'

# TCP fixture for queued responses, incomplete bodies, and connection reuse.
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
                 when 'mg' then @get_response.call(line, connection_id)
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
  def with_opaque_server(response, **options)
    server = OpaqueCorrelationServer.new(&response)
    client = Dalli::Client.new(server.address, raw: true, socket_timeout: 0.5,
                                               socket_failure_delay: nil, **options)
    # Bound regressions that accidentally drain a rejected body or retry.
    Timeout.timeout(3) { yield client, server }
  ensure
    client&.close
    server&.close
  end

  def assert_operation_miss(result, operation, expected)
    if operation == :get_with_status
      assert_predicate result, :miss?
    elsif expected.nil?
      assert_nil result
    else
      assert_equal expected, result
    end
  end

  def assert_disconnected(client)
    manager = client.send(:ring).servers.first.instance_variable_get(:@connection_manager)

    refute_predicate manager, :connected?
    refute_predicate manager, :request_in_progress?
  end

  it 'logs a mismatch and returns a miss without retrying or consuming queued responses' do
    response = lambda do |line, connection_id|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      if connection_id == 1
        "VA 5 Oobsolete\r\nstale\r\nVA 3 #{opaque}\r\n999\r\n"
      else
        "VA 5 f0 #{opaque}\r\nvalue\r\n"
      end
    end
    log = StringIO.new
    logger = Logger.new(log)
    logger.level = Logger::WARN

    Dalli.stub(:logger, logger) do
      with_opaque_server(response) do |client, server|
        assert_nil client.get('wanted')
        assert_disconnected(client)
        assert_equal 2, client.incr('counter')
        assert_equal 'value', client.get('good')
        assert_equal [[1, 'mg'], [2, 'ma'], [2, 'mg']], server.requests
      end
    end

    assert_includes log.string, 'Response correlation error: opaque mismatch (VA)'
  end

  it 'returns a miss for a rejected header without waiting for its declared body' do
    response = ->(_line, _connection_id) { "VA 1048576 Oobsolete\r\n" }

    with_opaque_server(response) do |client, server|
      assert_nil client.get('wanted')
      assert_disconnected(client)
      assert_equal 2, client.incr('counter')
      assert_equal [[1, 'mg'], [2, 'ma']], server.requests
    end
  end

  operations = [
    [:get, ['wanted'], nil],
    [:get, ['wanted', { cache_nils: true }], Dalli::NOT_FOUND],
    [:get, ['wanted', { meta_flags: ['t'] }], [nil, {}]],
    [:gat, ['wanted', 30], nil],
    [:gat, ['wanted', 30, { meta_flags: ['t'] }], [nil, {}]],
    [:get_cas, ['wanted'], [nil, 0]],
    [:get_with_status, ['wanted'], nil],
    [:touch, ['wanted', 30], nil]
  ]
  operations.each do |operation, args, expected|
    it "marks the peer down after repeated mismatches for #{operation}(#{args.inspect})" do
      response = lambda do |_line, _connection_id|
        operation == :touch ? "HD Oobsolete\r\n" : "VA 4 f1 Oobsolete\r\nNOPE\r\n"
      end

      with_opaque_server(response) do |client, server|
        # Reaching the failure limit still returns a miss; only later requests fail.
        2.times do
          result = client.public_send(operation, *args)

          assert_operation_miss(result, operation, expected)
          assert_disconnected(client)
        end

        assert_raises(Dalli::RingError) { client.get('another-key') }
        assert_equal [[1, 'mg'], [2, 'mg']], server.requests
      end
    end
  end

  %w[EN HD].each do |code|
    operations.each do |operation, args, expected|
      it "treats bare #{code} as a reusable miss for #{operation}(#{args.inspect})" do
        response = ->(_line, _connection_id) { "#{code}\r\n" }

        with_opaque_server(response) do |client, server|
          result = client.public_send(operation, *args)

          assert_operation_miss(result, operation, expected)
          assert_equal 2, client.incr('counter')
          assert_equal [[1, 'mg'], [1, 'ma']], server.requests
        end
      end
    end
  end

  [1, 3].each do |max_failures|
    it "honors socket_max_failures=#{max_failures} for value responses missing their opaque" do
      response = ->(_line, _connection_id) { "VA 4 f1\r\nNOPE\r\n" }

      with_opaque_server(response, socket_max_failures: max_failures) do |client, server|
        max_failures.times do
          assert_nil client.get('wanted')
          assert_disconnected(client)
        end

        assert_raises(Dalli::RingError) { client.get('wanted') }
        assert_equal (1..max_failures).map { |id| [id, 'mg'] }, server.requests
      end
    end
  end

  %w[EN HD].each do |code|
    it "returns a miss and closes the connection for #{code} with the wrong opaque" do
      response = lambda do |line, connection_id|
        opaque = line.split.find { |flag| flag.start_with?('O') }
        connection_id == 1 ? "#{code} Oobsolete\r\n" : "VA 5 f0 #{opaque}\r\nvalue\r\n"
      end

      with_opaque_server(response) do |client, server|
        assert_nil client.get('wanted')
        assert_disconnected(client)
        assert_equal 'value', client.get('wanted')
        assert_equal [[1, 'mg'], [2, 'mg']], server.requests
      end
    end
  end

  [
    [:get, ['wanted'], 'memcached.read'],
    [:gat, ['wanted', 30], 'memcached.gat'],
    [:get_with_status, ['wanted'], 'memcached.get_with_status']
  ].each do |operation, args, span_name|
    it "records #{operation} correlation failures as OpenTelemetry misses even when marking the peer down" do
      OTEL_EXPORTER.reset
      response = ->(_line, _connection_id) { "VA 4 Oobsolete\r\nNOPE\r\n" }

      options = { middlewares: [Dalli::OpentelemetryMiddleware], socket_max_failures: 1 }
      with_opaque_server(response, **options) do |client, server|
        result = client.public_send(operation, *args)

        assert_operation_miss(result, operation, nil)
        assert_disconnected(client)
        assert_equal [[1, 'mg']], server.requests
      end

      spans = OTEL_EXPORTER.finished_spans.select { |span| span.name == span_name }

      assert_equal 1, spans.size
      refute_equal OpenTelemetry::Trace::Status::ERROR, spans.first.status.code
      assert_equal 1, spans.first.attributes['miss_count']
      assert_equal 0, spans.first.attributes['hit_count']
      assert_equal 0, spans.first.attributes['value_bytesize']
    end
  end

  it 'resets failure and discard state after a successful response' do
    response = lambda do |line, _connection_id|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      line.split[1] == 'bad' ? "VA 4 Oobsolete\r\nNOPE\r\n" : "VA 5 f0 #{opaque}\r\nvalue\r\n"
    end

    with_opaque_server(response) do |client, server|
      2.times do
        assert_nil client.get('bad')
        assert_disconnected(client)
        assert_equal 'value', client.get('good')
      end
      assert_equal 'value', client.get('good')
      assert_equal [[1, 'mg'], [2, 'mg'], [2, 'mg'], [3, 'mg'], [3, 'mg']], server.requests
    end
  end

  it 'rejects caller opaque flags before sending a get or gat request' do
    response = ->(_line, _connection_id) { raise 'an invalid request must not be sent' }

    with_opaque_server(response) do |client, server|
      assert_raises(ArgumentError) { client.get('wanted', meta_flags: ['Ocaller']) }
      assert_raises(ArgumentError) { client.gat('wanted', 30, meta_flags: ['Ocaller']) }
      assert_empty server.requests
    end
  end
end
