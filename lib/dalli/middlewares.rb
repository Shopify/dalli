# frozen_string_literal: true

module Dalli
  # Basic middleware that does nothing.
  class BasicMiddleware
    def storage_req(_operation, _tags = {})
      attributes = {}.freeze
      yield attributes
    end

    def retrieve_req(_operation, _tags = {})
      attributes = {}.freeze
      yield attributes
    end

    # Diagnostic hooks run synchronously in retrieve_req, under the server lock when threadsafe.
    # With threadsafe: false, callers must provide exclusive client access.
    def record_request_opaque(_opaque); end

    def correlation_failure(_attributes); end

    def storage_req_pipeline(_operation, _tags = {})
      yield
    end

    def retrieve_req_pipeline(_operation, _tags = {})
      yield
    end
  end

  class Middlewares < BasicMiddleware
  end
end
