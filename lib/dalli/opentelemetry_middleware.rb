# frozen_string_literal: true

require 'opentelemetry'

module Dalli
  # Middleware to add OpenTelemetry spans to Dalli operations.
  module OpentelemetryMiddleware
    TRACER = OpenTelemetry.tracer_provider.tracer('dalli', Dalli::VERSION)

    DEFAULT_TRACE_ATTRIBUTES = {
      'db.system' => 'memcached'
    }.freeze

    def storage_req(operation, tags = {})
      TRACER.in_span(operation.to_s, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end

    def retrieve_req(operation, tags = {})
      TRACER.in_span(operation.to_s, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end

    def storage_req_pipeline(operation, tags = {})
      TRACER.in_span(operation.to_s, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end

    def retrieve_req_pipeline(operation, tags = {})
      TRACER.in_span(operation.to_s, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end
  end
end
