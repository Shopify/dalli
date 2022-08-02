# frozen_string_literal: true

module Dalli
  class Pipeline
    include Commands

    def initialize(ring, key_manager, options)
      @ring = ring
      @key_manager = key_manager
      @options = options
      @operations = []
      @index = -1
    end

    def execute
      return [] if @operations.empty?

      begin
        operations_by_server = group_operations
        responses = Array.new(@operations.size)

        @ring.lock do
          setup_requests(operations_by_server)
          start_time = Time.now
          servers = operations_by_server.keys
          servers = fetch_responses(servers, start_time, @ring.socket_timeout, &block) until servers.empty?
        end
      rescue NetworkError => e
        Dalli.logger.debug { e.inspect }
        Dalli.logger.debug { 'retrying pipelined gets because of timeout' }
        retry
      end
    end

    private

    def setup_requests(operations_by_server)
      operations_by_server.each do |server, operations|
        operations.each do |operation|
          server.request(operation.first)
        end
      end
    end

    def group_operations
      operations_by_server = {}
      @operations.each_with_index do |operation, index|
        key = operation[0][1]
        server_ops = operations_by_server[@ring.server_for_key(key)] ||= []
        server_ops << operation
      end
      operations_by_server
    end

    def perform(*args, &block)
      key = args[1].to_s
      args[1] = key = @key_manager.validate_key(key)

      index = (@index += 1)
      @operations << [args, block, @index]
      index
    end
  end
end
