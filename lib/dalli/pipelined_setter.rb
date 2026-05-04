# frozen_string_literal: true

module Dalli
  ##
  # Contains logic for the pipelined sets implemented by the client.
  ##
  class PipelinedSetter
    def initialize(ring)
      @ring = ring
    end

    ##
    # Writes multiple keys and values to the server.
    #
    # `req_options` is forwarded to the underlying multi-storage request and
    # applies to every entry in the pipeline (e.g. `:meta_flags`).
    #
    # NOTE: this pipelined path only supports single-server deployments.
    # For multi-server, `Dalli::Client#set_multi` falls back to a
    # `quiet { each set(..., req_options) }` loop, which still threads
    # `req_options` (and therefore `:meta_flags`) through on a per-key basis.
    ##
    def process(pairs, ttl, req_options = nil)
      return if pairs.empty?

      # Single server, no locking, and no grouping of pairs to server, performance optimization.
      # Note: groups_for_keys(pairs.keys) is slow, so we avoid it.
      raise 'not yet implemented' unless @ring.servers.length == 1

      @ring.servers.first.request(:write_multi_storage_req, pairs, ttl, req_options)
    rescue RetryableNetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying pipelined set because of timeout' }
      retry
    end
  end
end
