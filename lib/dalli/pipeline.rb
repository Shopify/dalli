# frozen_string_literal: true

module Dalli
  class Pipeline
    def initialize(ring, key_manager)
      @ring = ring
      @key_manager = key_manager
      @operations = []
    end

    def execute
      
    end
  end
end
