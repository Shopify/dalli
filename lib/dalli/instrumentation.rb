# frozen_string_literal: true

require 'opentelemetry'

module Dalli
  # Provides OpenTelemetry instrumentation for Dalli memcached operations
  module Instrumentation
    TRACER = OpenTelemetry.tracer_provider.tracer('memcached', Dalli::VERSION)

    DEFAULT_TRACE_ATTRIBUTES = {
      'db.system' => 'memcached'
    }.freeze

    class << self
      def instrument(operation, tags: {}, &)
        TRACER.in_span("memcached.#{operation}", attributes: DEFAULT_TRACE_ATTRIBUTES.merge(tags), kind: :client, &)
      end
    end
  end
end
