# frozen_string_literal: true

require_relative 'helper'
require 'dalli/opentelemetry_middleware'

describe 'OpenTelemetry middleware' do
  it 'emits OpenTelemetry spans when using the OpenTelemetry middleware' do
    OTEL_EXPORTER.reset if OTEL_EXPORTER.respond_to?(:reset)

    memcached(21_453, '', { middlewares: [Dalli::OpentelemetryMiddleware] }) do |dc, _|
      dc.set('otel:key', 'value', 5)
      _ = dc.get('otel:key')

      finished = OTEL_EXPORTER.respond_to?(:finished_spans) ? OTEL_EXPORTER.finished_spans : []
      names = finished.map(&:name)

      assert_includes names, 'memcached.write', "expected a 'write' span, got: #{names.inspect}"
      assert_includes names, 'memcached.read', "expected a 'read' span, got: #{names.inspect}"
    end
  end

  it 'includes value_bytesize, hit_count, and miss_count attributes on get operations' do
    OTEL_EXPORTER.reset if OTEL_EXPORTER.respond_to?(:reset)

    memcached(21_453, '', { middlewares: [Dalli::OpentelemetryMiddleware] }) do |dc, _|
      # Set a value and then get it (cache hit)
      test_value = 'test_value_123'
      dc.set('otel:hit_key', test_value, 5)
      result = dc.get('otel:hit_key')

      assert_equal test_value, result

      finished = OTEL_EXPORTER.respond_to?(:finished_spans) ? OTEL_EXPORTER.finished_spans : []
      read_span = finished.find { |span| span.name == 'memcached.read' }

      refute_nil read_span, 'expected to find a memcached.read span'

      attributes = read_span.attributes
      # value_bytesize reports the marshalled size from memcached
      assert_operator attributes['value_bytesize'], :>=, test_value.bytesize,
                      "expected value_bytesize to be at least #{test_value.bytesize}"
      assert_equal 1, attributes['hit_count'], 'expected hit_count to be 1 for cache hit'
      assert_equal 0, attributes['miss_count'], 'expected miss_count to be 0 for cache hit'
    end
  end

  it 'includes correct attributes for cache miss on get operations' do
    OTEL_EXPORTER.reset if OTEL_EXPORTER.respond_to?(:reset)

    memcached(21_453, '', { middlewares: [Dalli::OpentelemetryMiddleware] }) do |dc, _|
      # Get a non-existent key (cache miss)
      result = dc.get('otel:nonexistent_key')

      assert_nil result

      finished = OTEL_EXPORTER.respond_to?(:finished_spans) ? OTEL_EXPORTER.finished_spans : []
      read_span = finished.find { |span| span.name == 'memcached.read' }

      refute_nil read_span, 'expected to find a memcached.read span'

      attributes = read_span.attributes

      assert_equal 0, attributes['value_bytesize'], 'expected value_bytesize to be 0 for cache miss'
      assert_equal 0, attributes['hit_count'], 'expected hit_count to be 0 for cache miss'
      assert_equal 1, attributes['miss_count'], 'expected miss_count to be 1 for cache miss'
    end
  end

  it 'includes value_bytesize, hit_count, and miss_count attributes on gat operations' do
    OTEL_EXPORTER.reset if OTEL_EXPORTER.respond_to?(:reset)

    memcached(21_453, '', { middlewares: [Dalli::OpentelemetryMiddleware] }) do |dc, _|
      # Set a value and then gat it
      test_value = 'gat_test_value'
      dc.set('otel:gat_key', test_value, 5)
      result = dc.gat('otel:gat_key', 10)

      assert_equal test_value, result

      finished = OTEL_EXPORTER.respond_to?(:finished_spans) ? OTEL_EXPORTER.finished_spans : []
      gat_span = finished.find { |span| span.name == 'memcached.gat' }

      refute_nil gat_span, 'expected to find a memcached.gat span'

      attributes = gat_span.attributes
      # value_bytesize reports the marshalled size from memcached
      assert_operator attributes['value_bytesize'], :>=, test_value.bytesize,
                      "expected value_bytesize to be at least #{test_value.bytesize}"
      assert_equal 1, attributes['hit_count'], 'expected hit_count to be 1 for cache hit'
      assert_equal 0, attributes['miss_count'], 'expected miss_count to be 0 for cache hit'
    end
  end

  it 'includes value_bytesize, hit_count, and miss_count attributes on read_multi operations' do
    OTEL_EXPORTER.reset if OTEL_EXPORTER.respond_to?(:reset)

    memcached(21_453, '', { middlewares: [Dalli::OpentelemetryMiddleware] }) do |dc, _|
      # Set multiple values
      dc.set('otel:multi1', 'value1', 5)
      dc.set('otel:multi2', 'value2', 5)
      dc.set('otel:multi3', 'value3', 5)

      # Get multiple keys with some hits and some misses
      results = dc.get_multi('otel:multi1', 'otel:multi2', 'otel:nonexistent', 'otel:multi3')

      assert_equal 3, results.size
      assert_equal 'value1', results['otel:multi1']
      assert_equal 'value2', results['otel:multi2']
      assert_equal 'value3', results['otel:multi3']

      finished = OTEL_EXPORTER.respond_to?(:finished_spans) ? OTEL_EXPORTER.finished_spans : []
      read_multi_span = finished.find { |span| span.name == 'memcached.read_multi' }

      refute_nil read_multi_span, 'expected to find a memcached.read_multi span'

      attributes = read_multi_span.attributes
      # value_bytesize includes marshalled size which can be larger than raw strings
      assert_operator attributes['value_bytesize'], :>, 0,
                      'expected value_bytesize to be greater than 0'
      assert_equal 3, attributes['hit_count'], 'expected hit_count to be 3 (three hits)'
      assert_equal 1, attributes['miss_count'], 'expected miss_count to be 1 (one miss)'
    end
  end
end
