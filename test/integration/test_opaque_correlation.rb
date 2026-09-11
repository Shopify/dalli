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

  def response_for(command, line, connection_id)
    case command
    when 'version' then "VERSION 1.6.41-fake\r\n"
    when 'mg' then @get_response.call(line, connection_id)
    when 'ma' then "VA 1\r\n2\r\n"
    else raise "Unexpected command: #{line.inspect}"
    end
  end

  def serve(connection, connection_id)
    while (line = connection.gets("\r\n"))
      command = line.split.first
      @requests << [connection_id, command] unless command == 'version'
      response = response_for(command, line, connection_id)
      break if response.nil?

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

  it 'keeps real memcached connections open for correlated VA, EN, and HD responses' do
    memcached_persistent do |client|
      client.set('opaque-hit', 'value')
      client.delete('opaque-miss')
      server = client.send(:ring).servers.first
      socket = server.sock

      refute_nil socket
      assert_equal 'value', client.get('opaque-hit')
      assert_same socket, server.sock
      assert_nil client.get('opaque-miss')
      assert_same socket, server.sock
      assert client.touch('opaque-hit', 30)
      assert_same socket, server.sock
    end
  end

  it 'logs a mismatch and returns a miss without retrying or consuming queued responses' do
    expected_opaque = nil
    response = lambda do |line, connection_id|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      expected_opaque ||= opaque.delete_prefix('O')
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

    assert_equal 1, log.string.scan('event=dalli.response_correlation_mismatch').size
    assert_match(/server="127\.0\.0\.1:\d+"/, log.string)
    assert_includes log.string, 'response_code=VA reason=mismatch'
    assert_includes log.string, "expected_opaque=#{expected_opaque.inspect}"
    assert_includes log.string, 'received_opaque="obsolete" received_opaque_bytes=8'
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
    it "recycles connections without marking the peer down for #{operation}(#{args.inspect})" do
      response = lambda do |_line, _connection_id|
        operation == :touch ? "HD Oobsolete\r\n" : "VA 4 f1 Oobsolete\r\nNOPE\r\n"
      end

      with_opaque_server(response, socket_max_failures: 1) do |client, server|
        3.times do
          result = client.public_send(operation, *args)

          assert_operation_miss(result, operation, expected)
          assert_disconnected(client)
        end

        assert_equal [[1, 'mg'], [2, 'mg'], [3, 'mg']], server.requests
      end
    end
  end

  %w[EN HD].each do |code|
    operations.each do |operation, args, expected|
      it "discards bare #{code} and any queued reply for #{operation}(#{args.inspect})" do
        response = lambda do |line, _connection_id|
          opaque = line.split.find { |flag| flag.start_with?('O') }
          queued = operation == :touch ? "HD #{opaque}\r\n" : "VA 3 #{opaque}\r\n999\r\n"
          "#{code}\r\n#{queued}"
        end

        with_opaque_server(response) do |client, server|
          result = client.public_send(operation, *args)

          assert_operation_miss(result, operation, expected)
          assert_disconnected(client)
          assert_equal 2, client.incr('counter')
          assert_equal [[1, 'mg'], [2, 'ma']], server.requests
        end
      end
    end
  end

  it 'only closes the connection for a missing opaque even with socket_max_failures: 1' do
    response = ->(_line, _connection_id) { "VA 4 f1\r\nNOPE\r\n" }

    with_opaque_server(response, socket_max_failures: 1) do |client, server|
      2.times do
        assert_nil client.get('wanted')
        assert_disconnected(client)
      end
      assert_equal 2, client.incr('counter')
      assert_equal [[1, 'mg'], [2, 'mg'], [3, 'ma']], server.requests
    end
  end

  it 'preserves ordinary socket-error retries across successful reconnects' do
    response = lambda do |line, connection_id|
      next nil if connection_id <= 2

      opaque = line.split.find { |flag| flag.start_with?('O') }
      "EN #{opaque}\r\n"
    end

    with_opaque_server(response) do |client, server|
      assert_nil client.get('wanted')
      assert_equal 2, client.incr('counter')
      assert_equal [[1, 'mg'], [2, 'mg'], [3, 'mg'], [3, 'ma']], server.requests
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
    it "records #{operation} correlation failures as OpenTelemetry misses" do
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
      assert_equal 1, spans.first.attributes['correlation_mismatch']
      assert_equal 'mismatch', spans.first.attributes['correlation_failure_reason']
      assert_equal 'VA', spans.first.attributes['response_code']
      assert_match(/\A[A-Za-z0-9_-]{11}\z/, spans.first.attributes['request_opaque'])
      assert_equal 'obsolete', spans.first.attributes['received_opaque']
    end
  end

  [
    [:get, ['wanted'], 'memcached.read', 'value'],
    [:gat, ['wanted', 30], 'memcached.gat', 'value'],
    [:get_cas, ['wanted'], 'memcached.cas', ['value', 7]],
    [:get_with_status, ['wanted'], 'memcached.get_with_status', 'value'],
    [:touch, ['wanted', 30], 'memcached.touch', true]
  ].each do |operation, args, span_name, expected|
    it "records the wire opaque on successful #{operation} spans" do
      OTEL_EXPORTER.reset
      opaques = []
      response = lambda do |line, _connection_id|
        flags = line.split
        opaque = flags.find { |flag| flag.start_with?('O') }
        opaques << opaque.delete_prefix('O')
        flags.include?('v') ? "VA 5 f0 c7 #{opaque}\r\nvalue\r\n" : "HD #{opaque}\r\n"
      end

      with_opaque_server(response, middlewares: [Dalli::OpentelemetryMiddleware]) do |client, _server|
        2.times do
          result = client.public_send(operation, *args)
          result = result.value if operation == :get_with_status

          assert_equal expected, result
        end
      end
      spans = OTEL_EXPORTER.finished_spans.select { |span| span.name == span_name }

      assert_equal 2, opaques.uniq.size
      assert_equal(opaques, spans.map { |span| span.attributes['request_opaque'] })
      spans.each { |span| refute span.attributes.key?('correlation_mismatch') }
    end
  end

  [nil, '', ("x\"\\\x00\xff".b * 10)].each do |received|
    it "logs and traces bounded mismatch details for #{received.inspect}" do
      OTEL_EXPORTER.reset
      log = StringIO.new
      logger = Logger.new(log)
      logger.level = Logger::WARN
      logger.formatter = ->(_severity, _time, _progname, message) { "#{message}\n" }
      flag = received.nil? ? '' : " O#{received}"
      response = ->(_line, _connection_id) { "VA 4#{flag}\r\nNOPE\r\n" }

      Dalli.stub(:logger, logger) do
        with_opaque_server(response, middlewares: [Dalli::OpentelemetryMiddleware]) do |client, _server|
          assert_nil client.get('wanted')
          assert_disconnected(client)
        end
      end
      attributes = OTEL_EXPORTER.finished_spans.find { |span| span.name == 'memcached.read' }.attributes
      preview = received&.byteslice(0, 32)
      size = received&.bytesize || 0
      reason = received.nil? ? 'missing' : 'mismatch'

      assert_equal 1, log.string.lines.size
      assert_includes log.string, 'event=dalli.response_correlation_mismatch'
      assert_includes log.string, "expected_opaque=#{attributes['request_opaque'].inspect}"
      assert_includes log.string, "received_opaque=#{preview.inspect} received_opaque_bytes=#{size}"
      assert_equal reason, attributes['correlation_failure_reason']
      assert_equal size, attributes['received_opaque_bytes']
      if preview.nil?
        refute attributes.key?('received_opaque')
      else
        assert_equal preview.encode(Encoding::UTF_8, invalid: :replace, undef: :replace), attributes['received_opaque']
      end
    end
  end

  it 'clears discard state before subsequent requests' do
    OTEL_EXPORTER.reset
    response = lambda do |line, _connection_id|
      opaque = line.split.find { |flag| flag.start_with?('O') }
      line.split[1] == 'bad' ? "VA 4 Oobsolete\r\nNOPE\r\n" : "VA 5 f0 #{opaque}\r\nvalue\r\n"
    end

    with_opaque_server(response, middlewares: [Dalli::OpentelemetryMiddleware]) do |client, server|
      2.times do
        assert_nil client.get('bad')
        assert_disconnected(client)
        assert_equal 'value', client.get('good')
      end
      assert_equal 'value', client.get('good')
      assert_equal [[1, 'mg'], [2, 'mg'], [2, 'mg'], [3, 'mg'], [3, 'mg']], server.requests
    end
    spans = OTEL_EXPORTER.finished_spans.select { |span| span.name == 'memcached.read' }

    assert_equal([1, nil, 1, nil, nil], spans.map { |span| span.attributes['correlation_mismatch'] })
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
