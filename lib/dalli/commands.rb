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
      perform(:touch, key, ttl_or_default(ttl)) do |resp|
        resp.nil? ? nil : true
      end
    end

    ##
    # Uses the argument TTL or the client-wide default.  Ensures
    # that the value is an integer
    ##
    def ttl_or_default(ttl)
      (ttl || @options[:expires_in]).to_i
    rescue NoMethodError
      raise ArgumentError, "Cannot convert ttl (#{ttl}) to an integer"
    end
  end
end

