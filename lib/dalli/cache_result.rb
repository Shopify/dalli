# frozen_string_literal: true

module Dalli
  # Result of a status-aware read (Client#get_with_status or Client#get_cas).
  # Callers should branch on the predicate methods, not on nil-ness, since a
  # tombstoned item has stale? == true with a (possibly empty) value, while
  # a real cache miss has miss? == true. CAS reads also expose an opaque token
  # through cas_token; other read operations leave it nil.
  class CacheResult
    attr_reader :cas_token, :value

    def initialize(value:, stale: false, miss: false, cas_token: nil)
      @value = value
      @stale = stale
      @miss = miss
      @cas_token = cas_token
      freeze
    end

    def stale?
      @stale
    end

    def miss?
      @miss
    end

    def hit?
      !@miss
    end
  end
end
