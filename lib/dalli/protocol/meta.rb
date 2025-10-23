# frozen_string_literal: true

require 'forwardable'
require 'socket'
require 'timeout'

module Dalli
  module Protocol
    ##
    # Access point for a single Memcached server, accessed via Memcached's meta
    # protocol.  Contains logic for managing connection state to the server (retries, etc),
    # formatting requests to the server, and unpacking responses.
    ##
    class Meta < Base
      TERMINATOR = "\r\n"
      SUPPORTS_CAPACITY = Gem::Version.new(RUBY_VERSION) >= Gem::Version.new('3.4.0')

      def response_processor
        @response_processor ||= ResponseProcessor.new(@connection_manager, @value_marshaller)
      end

      private

      # * only supports single server
      # * only supports set at the moment
      # * doesn't support cas at the moment
      # rubocop:disable Metrics/AbcSize
      def write_multi_storage_req(pairs, ttl = nil, options = {})
        ttl = TtlSanitizer.sanitize(ttl) if ttl

        @middlewares_stack.storage_req_pipeline('write_multi', {
                                                  'keys' => pairs.keys,
                                                  'ttl' => ttl
                                                }) do |attributes|
          total_value_bytesize = 0
          pairs.each do |key, raw_value|
            (value, bitflags) = @value_marshaller.store(key, raw_value, options)
            encoded_key, _base64 = KeyRegularizer.encode(key)
            encoded_key = @key_manager.validate_key(encoded_key)

            value_bytesize = value.bytesize
            total_value_bytesize += value_bytesize
            # if last pair of hash, add TERMINATOR

            # NOTE: The most efficient way to build the command
            # * avoid making new strings capacity is set to the max possible size of the command
            # * socket write each piece indepentantly to avoid extra allocation
            # * first chunk uses interpolated values to avoid extra allocation, but << for larger 'value' strings
            # * avoids using the request formatter pattern for single inline builder
            @connection_manager.write("ms #{encoded_key} #{value_bytesize} c F#{bitflags} T#{ttl} MS q\r\n")
            @connection_manager.write(value)
            @connection_manager.write(TERMINATOR)
          end
          attributes['value_size'] = total_value_bytesize unless attributes.frozen?

          write_noop
          @connection_manager.flush

          response_processor.consume_all_responses_until_mn
        end
      end

      # rubocop:disable Metrics/CyclomaticComplexity
      # rubocop:disable Metrics/PerceivedComplexity
      # rubocop:disable Metrics/MethodLength
      def read_multi_req(keys)
        # Pre-allocate the results hash with expected size
        results = SUPPORTS_CAPACITY ? Hash.new(nil, capacity: keys.size) : {}
        optimized_for_raw = @value_marshaller.raw_by_default
        key_index = optimized_for_raw ? 2 : 3

        total_value_bytesize = 0
        @middlewares_stack.retrieve_req_pipeline('read_multi', { 'keys' => keys }) do |attributes|
          post_get_req = optimized_for_raw ? "v k q\r\n" : "v f k q\r\n"
          keys.each do |key|
            @connection_manager.write("mg #{key} #{post_get_req}")
          end
          @connection_manager.write("mn\r\n")
          @connection_manager.flush

          terminator_length = TERMINATOR.length
          while (line = @connection_manager.readline)
            break if line == TERMINATOR || line[0, 2] == 'MN'
            next unless line[0, 3] == 'VA '

            # VA value_length flags key
            tokens = line.split
            value = @connection_manager.read_exact(tokens[1].to_i)
            bitflags = optimized_for_raw ? 0 : @response_processor.bitflags_from_tokens(tokens)
            @connection_manager.read_exact(terminator_length) # read the terminator
            key = tokens[key_index]&.byteslice(1..-1)
            next if key.nil?

            total_value_bytesize += value.bytesize
            results[key] = @value_marshaller.retrieve(value, bitflags)
          end

          unless attributes.frozen?
            attributes['value_bytesize'] = total_value_bytesize
            attributes['hit_count'] = results.size
            attributes['miss_count'] = keys.size - results.size
          end
        end

        results
      end
      # rubocop:enable Metrics/CyclomaticComplexity
      # rubocop:enable Metrics/PerceivedComplexity
      # rubocop:enable Metrics/MethodLength

      def delete_multi_req(keys)
        encoded_keys = []

        @middlewares_stack.storage_req_pipeline('delete_multi', { 'keys' => keys }) do
          keys.each do |key|
            encoded_key, base64 = KeyRegularizer.encode(key)
            encoded_keys << encoded_key
            req = RequestFormatter.meta_delete(key: encoded_key, base64: base64, quiet: true)
            write(req)
          end
          write_noop
          @connection_manager.flush

          response_processor.consume_all_responses_until_mn
        end
      end

      # rubocop:enable Metrics/AbcSize
      # Retrieval Commands
      # rubocop:disable Metrics/AbcSize
      # rubocop:disable Metrics/CyclomaticComplexity
      # rubocop:disable Metrics/PerceivedComplexity
      def get(key, options = nil)
        encoded_key, base64 = KeyRegularizer.encode(key)
        meta_options = meta_flag_options(options)

        @middlewares_stack.retrieve_req('read', { 'keys' => key }) do
          if !meta_options && !base64 && !quiet? && @value_marshaller.raw_by_default
            write("mg #{encoded_key} v\r\n")
          else
            write(RequestFormatter.meta_get(key: encoded_key, base64: base64, meta_flags: meta_options))
          end
          @connection_manager.flush
          if !meta_options && !base64 && !quiet? && @value_marshaller.raw_by_default
            response_processor.meta_get_with_value(cache_nils: cache_nils?(options), skip_flags: true)
          elsif meta_options
            response_processor.meta_get_with_value_and_meta_flags(cache_nils: cache_nils?(options))
          else
            response_processor.meta_get_with_value(cache_nils: cache_nils?(options))
          end
        end
      end
      # rubocop:enable Metrics/AbcSize
      # rubocop:enable Metrics/CyclomaticComplexity
      # rubocop:enable Metrics/PerceivedComplexity

      def quiet_get_request(key)
        encoded_key, base64 = KeyRegularizer.encode(key)
        @middlewares_stack.retrieve_req('read', { 'keys' => key, 'quiet' => true }) do
          RequestFormatter.meta_get(key: encoded_key, return_cas: true, base64: base64, quiet: true)
        end
      end

      def gat(key, ttl, options = nil)
        ttl = TtlSanitizer.sanitize(ttl)
        encoded_key, base64 = KeyRegularizer.encode(key)

        @middlewares_stack.retrieve_req('gat', { 'keys' => key, 'ttl' => ttl }) do
          req = RequestFormatter.meta_get(key: encoded_key, ttl: ttl, base64: base64,
                                          meta_flags: meta_flag_options(options))
          write(req)
          @connection_manager.flush
          if meta_flag_options(options)
            response_processor.meta_get_with_value_and_meta_flags(cache_nils: cache_nils?(options))
          else
            response_processor.meta_get_with_value(cache_nils: cache_nils?(options))
          end
        end
      end

      def touch(key, ttl)
        ttl = TtlSanitizer.sanitize(ttl)
        encoded_key, base64 = KeyRegularizer.encode(key)

        @middlewares_stack.retrieve_req('touch', { 'keys' => key, 'ttl' => ttl }) do
          req = RequestFormatter.meta_get(key: encoded_key, ttl: ttl, value: false, base64: base64)
          write(req)
          @connection_manager.flush
          response_processor.meta_get_without_value
        end
      end

      # TODO: This is confusing, as there's a cas command in memcached
      # and this isn't it.  Maybe rename?  Maybe eliminate?
      def cas(key)
        encoded_key, base64 = KeyRegularizer.encode(key)

        @middlewares_stack.retrieve_req('cas', { 'keys' => key }) do
          req = RequestFormatter.meta_get(key: encoded_key, value: true, return_cas: true, base64: base64)
          write(req)
          @connection_manager.flush
          response_processor.meta_get_with_value_and_cas
        end
      end

      # Storage Commands
      def set(key, value, ttl, cas, options)
        do_storage_req(:write, key, value, ttl, cas, options)
      end

      def add(key, value, ttl, options)
        do_storage_req(:add, key, value, ttl, nil, options)
      end

      def replace(key, value, ttl, cas, options)
        do_storage_req(:replace, key, value, ttl, cas, options)
      end

      # rubocop:disable Metrics/ParameterLists
      def do_storage_req(mode, key, raw_value, ttl = nil, cas = nil, options = {})
        (value, bitflags) = @value_marshaller.store(key, raw_value, options)
        ttl = TtlSanitizer.sanitize(ttl) if ttl
        encoded_key, base64 = KeyRegularizer.encode(key)
        @middlewares_stack.storage_req(mode.to_s, { 'keys' => key, 'value_size' => value.bytesize, 'ttl' => ttl }) do
          req = RequestFormatter.meta_set(key: encoded_key, value: value,
                                          bitflags: bitflags, cas: cas,
                                          ttl: ttl, mode: mode, quiet: quiet?, base64: base64)
          write(req)
          write(value)
          write(TERMINATOR)
          @connection_manager.flush

          response_processor.meta_set_with_cas unless quiet?
        end
      end
      # rubocop:enable Metrics/ParameterLists

      def append(key, value)
        @middlewares_stack.storage_req('append', { 'keys' => key, 'value_size' => value.bytesize }) do
          write_append_prepend_req(:append, key, value)
          response_processor.meta_set_append_prepend unless quiet?
        end
      end

      def prepend(key, value)
        @middlewares_stack.storage_req('prepend', { 'keys' => key, 'value_size' => value.bytesize }) do
          write_append_prepend_req(:prepend, key, value)
          response_processor.meta_set_append_prepend unless quiet?
        end
      end

      # rubocop:disable Metrics/ParameterLists
      def write_append_prepend_req(mode, key, value, ttl = nil, cas = nil, _options = {})
        ttl = TtlSanitizer.sanitize(ttl) if ttl
        encoded_key, base64 = KeyRegularizer.encode(key)
        req = RequestFormatter.meta_set(key: encoded_key, value: value, base64: base64,
                                        cas: cas, ttl: ttl, mode: mode, quiet: quiet?)
        write(req)
        write(value)
        write(TERMINATOR)
        @connection_manager.flush
      end
      # rubocop:enable Metrics/ParameterLists

      # Delete Commands
      def delete(key, cas)
        encoded_key, base64 = KeyRegularizer.encode(key)
        @middlewares_stack.storage_req('delete', { 'keys' => key, 'cas' => cas }) do
          req = RequestFormatter.meta_delete(key: encoded_key, cas: cas,
                                             base64: base64, quiet: quiet?)
          write(req)
          @connection_manager.flush
          response_processor.meta_delete unless quiet?
        end
      end

      # Arithmetic Commands
      def decr(key, count, ttl, initial)
        decr_incr false, key, count, ttl, initial
      end

      def incr(key, count, ttl, initial)
        decr_incr true, key, count, ttl, initial
      end

      def decr_incr(incr, key, delta, ttl, initial)
        ttl = initial ? TtlSanitizer.sanitize(ttl) : nil # Only set a TTL if we want to set a value on miss
        encoded_key, base64 = KeyRegularizer.encode(key)

        @middlewares_stack.storage_req(
          incr ? 'incr' : 'decr',
          {
            'keys' => key,
            'delta' => delta,
            'ttl' => ttl || 0,
            'initial' => initial || 0
          }
        ) do
          write(RequestFormatter.meta_arithmetic(key: encoded_key, delta: delta, initial: initial, incr: incr, ttl: ttl,
                                                 quiet: quiet?, base64: base64))
          @connection_manager.flush
          response_processor.decr_incr unless quiet?
        end
      end

      # Other Commands
      def flush(delay = 0)
        @middlewares_stack.storage_req('flush', { 'delay' => delay }) do
          write(RequestFormatter.flush(delay: delay))
          @connection_manager.flush
          response_processor.flush unless quiet?
        end
      end

      # Noop is a keepalive operation but also used to demarcate the end of a set of pipelined commands.
      # We need to read all the responses at once.
      def noop
        write_noop
        @connection_manager.flush
        response_processor.consume_all_responses_until_mn
      end

      def stats(info = nil)
        write(RequestFormatter.stats(info))
        @connection_manager.flush
        response_processor.stats
      end

      def reset_stats
        write(RequestFormatter.stats('reset'))
        @connection_manager.flush
        response_processor.reset
      end

      def version
        @middlewares_stack.retrieve_req('version') do
          write(RequestFormatter.version)
          @connection_manager.flush
          response_processor.version
        end
      end

      def write_noop
        write(RequestFormatter.meta_noop)
        @connection_manager.flush
      end

      def authenticate_connection
        raise Dalli::DalliError, 'Authentication not supported for the meta protocol.'
      end

      require_relative 'meta/key_regularizer'
      require_relative 'meta/request_formatter'
      require_relative 'meta/response_processor'
    end
  end
end
