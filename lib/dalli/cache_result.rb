# frozen_string_literal: true

module Dalli
  # Result of a stale-aware read (Client#get_with_status). Always returned —
  # callers should branch on the predicate methods, not on nil-ness, since a
  # tombstoned item has stale? == true with a (possibly empty) value, while
  # a real cache miss has miss? == true.
  class CacheResult
    attr_reader :value

    def initialize(value:, stale: false, miss: false)
      @value = value
      @stale = stale
      @miss = miss
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
