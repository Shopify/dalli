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
end
