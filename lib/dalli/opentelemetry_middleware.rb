# frozen_string_literal: true

require 'opentelemetry'

module Dalli
  # Middleware to add OpenTelemetry spans to Dalli operations.
  module OpentelemetryMiddleware
    TRACER = OpenTelemetry.tracer_provider.tracer('dalli', Dalli::VERSION)

    DEFAULT_TRACE_ATTRIBUTES = {
      'db.system' => 'memcached'
    }.freeze

    def storage_req(operation, tags = {}, &)
      traced_request(operation, tags, &)
    end

    def retrieve_req(operation, tags = {}, &)
      traced_request(operation, tags, &)
    end

    # Keep unique tokens out of metadata shared with general metrics middleware.
    def record_request_opaque(opaque)
      @request_trace_attributes['request_opaque'] = opaque if @request_trace_attributes
    end

    def correlation_failure(attributes)
      @request_trace_attributes&.merge!(attributes)
    end

    def storage_req_pipeline(operation, tags = {})
      TRACER.in_span(operation, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end

    def retrieve_req_pipeline(operation, tags = {})
      TRACER.in_span(operation, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        result = yield attributes
        span.add_attributes(attributes)
        result
      end
    end

    private

    # Record computed attributes only on normal completion, matching the other hooks.
    def traced_request(operation, tags)
      TRACER.in_span(operation, attributes: tags.merge!(DEFAULT_TRACE_ATTRIBUTES), kind: :client) do |span|
        attributes = {}
        trace_attributes = {}
        # Preserve outer state for reentrant instrumentation, not overlapping socket requests.
        previous_trace_attributes = @request_trace_attributes
        begin
          @request_trace_attributes = trace_attributes
          result = yield attributes
          span.add_attributes(attributes)
          span.add_attributes(trace_attributes) unless trace_attributes.empty?
          result
        ensure
          @request_trace_attributes = previous_trace_attributes
        end
      end
    end
  end
end
