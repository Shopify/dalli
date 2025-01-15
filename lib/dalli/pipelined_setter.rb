# frozen_string_literal: true

module Dalli
  ##
  # Contains logic for the pipelined sets implemented by the client.
  ##
  class PipelinedSetter
    def initialize(ring, key_manager)
      @ring = ring
      @key_manager = key_manager
    end

    ##
    # Writes multiple keys and values to the server.
    ##
    def process(pairs, ttl, req_options = nil)
      return if pairs.empty?

      # Single server, no locking, and no grouping of pairs to server, performance optimization.
      # Note: groups_for_keys(pairs.keys) is slow, so we avoid it.
      raise 'not yet implemented' unless @ring.servers.length == 1

      @ring.servers.first.request(:write_multi_storage_req, :set, pairs, ttl, 0, req_options)
    rescue RetryableNetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying pipelined gets because of timeout' }
      retry
    end
  end
end
