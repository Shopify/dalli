# frozen_string_literal: true

module Dalli
  ##
  # Contains logic for the pipelined deletes implemented by the client.
  ##
  class PipelinedDeleter
    def initialize(ring, key_manager)
      @ring = ring
      @key_manager = key_manager
    end

    ##
    # Deletes multiple keys from the server.
    # Returns the number of keys that were successfully deleted.
    #
    # `req_options` is an optional hash of options applied to every delete
    # in the pipeline (e.g. :meta_flags). Best-effort; caller is responsible
    # for ensuring options are appropriate for every key.
    ##
    def process(keys, req_options = nil)
      return 0 if keys.empty?

      # Validate and prepare keys
      validated_keys = keys.map { |key| @key_manager.validate_key(key.to_s) }

      # Single server optimization
      raise 'Multi-server pipelined delete not yet implemented' unless @ring.servers.length == 1

      @ring.servers.first.request(:delete_multi_req, validated_keys, req_options)

    # Multi-server not yet implemented for pipelined delete
    rescue RetryableNetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying pipelined delete because of timeout' }
      retry
    end
  end
end
