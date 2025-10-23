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
    # Returns a hash with keys mapped to true (deleted) or false (not found).
    ##
    def process(keys)
      return {} if keys.empty?

      # Validate and prepare keys
      validated_keys = keys.map { |key| @key_manager.validate_key(key.to_s) }

      # Single server optimization
      raise 'Multi-server pipelined delete not yet implemented' unless @ring.servers.length == 1

      @ring.servers.first.request(:delete_multi_req, validated_keys)

    # Multi-server not yet implemented for pipelined delete
    rescue RetryableNetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying pipelined delete because of timeout' }
      retry
    end
  end
end
