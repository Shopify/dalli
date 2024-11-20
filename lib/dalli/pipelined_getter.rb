# frozen_string_literal: true

module Dalli
  ##
  # Contains logic for the pipelined gets implemented by the client.
  ##
  class PipelinedGetter
    def initialize(ring, key_manager)
      @ring = ring
      @key_manager = key_manager
    end

    def process(keys, &block)
      return {} if keys.empty?

      results = {}
      @ring.lock do
        servers = groups_for_keys(keys)
        results = perform_requests(servers, &block)
      end

      results.each do |key, value|
        yield @key_manager.key_without_namespace(key), value
      end

      results
    rescue NetworkError => e
      Dalli.logger.debug { e.inspect }
      Dalli.logger.debug { 'retrying pipelined gets because of timeout' }
      retry
    end

    def perform_requests(keys)
      puts 'perform_requests'
      groups = groups_for_keys(keys)
      results = {}
      lock = Mutex.new if @ring.threadsafe?
      threads = [] if @ring.threadsafe?
      groups.each do |server, keys_for_server|
        puts 'perform_requests loop'
        if @ring.threadsafe? && @ring.servers.count > 1
          thread = Thread.new do
            threaded_results = server.request(:read_multi_req, keys_for_server)
            lock.synchronize do
              results.merge!(threaded_results)
            end
          rescue DalliError, NetworkError => e
            Dalli.logger.debug { e.inspect }
            Dalli.logger.debug { "unable to get keys for server #{server.name}" }
          end
          threads << thread
        else
          begin
            results.merge!(server.request(:read_multi_req, keys_for_server))
          rescue DalliError, NetworkError => e
            Dalli.logger.debug { e.inspect }
            Dalli.logger.debug { "unable to get keys for server #{server.name}" }
          end
        end
      end

      threads.each(&:join) if @ring.threadsafe?
      if block_given?
        results.each do |key, value|
          yield @key_manager.key_without_namespace(key), value
        end
      end

      results
    end

    def groups_for_keys(*keys)
      keys.map! { |a| @key_manager.validate_key(a.to_s) }
      groups = @ring.keys_grouped_by_server(keys)
      if (unfound_keys = groups.delete(nil))
        Dalli.logger.debug do
          "unable to get keys for #{unfound_keys.length} keys " \
            'because no matching server was found'
        end
      end
      groups
    end
  end
end
