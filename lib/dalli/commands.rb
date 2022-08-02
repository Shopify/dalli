# frozen_string_literal: true

module Dalli
  module Commands
    #
    # The standard memcached instruction set
    #

    ##
    # Get the value associated with the key.
    # If a value is not found, then +nil+ is returned.
    def get(key, req_options = nil)
      perform(:get, key, req_options)
    end

    ##
    # Gat (get and touch) fetch an item and simultaneously update its expiration time.
    #
    # If a value is not found, then +nil+ is returned.
    def gat(key, ttl = nil)
      perform(:gat, key, ttl_or_default(ttl))
    end

    ##
    # Touch updates expiration time for a given key.
    #
    # Returns true if key exists, otherwise nil.
    def touch(key, ttl = nil)
      resp = perform(:touch, key, ttl_or_default(ttl))
      resp.nil? ? nil : true
    end
  end
end

