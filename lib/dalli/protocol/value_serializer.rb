# frozen_string_literal: true

module Dalli
  module Protocol
    ##
    # Dalli::Protocol::ValueSerializer compartmentalizes the logic for managing
    # serialization and deserialization of stored values.  It manages interpreting
    # relevant options from both client and request, determining whether to
    # serialize/deserialize on store/retrieve, and processes bitflags as necessary.
    ##
    class ValueSerializer
      DEFAULTS = {
        raw: false,
        cache_nils: false,
        serializer: Marshal
      }.freeze

      OPTIONS = DEFAULTS.keys.freeze

      # https://www.hjp.at/zettel/m/memcached_flags.rxml
      # Looks like most clients use bit 0 to indicate native language serialization
      FLAG_SERIALIZED = 0x1

      attr_accessor :serialization_options

      def initialize(protocol_options)
        @serialization_options =
          DEFAULTS.merge(protocol_options.select { |k, _| OPTIONS.include?(k) })
      end

      def store(value, req_options, bitflags)
        do_serialize = serialize_value?(req_options)
        store_value = if value.nil?
          if @serialization_options[:cache_nils]
            value.to_s
          else
            raise ArgumentError, "value cannot be nil unless cache_nils option is enabled"
          end
        else
          do_serialize ? serialize_value(value) : value.to_s
        end
        bitflags |= FLAG_SERIALIZED if do_serialize
        [store_value, bitflags]
      end

      def retrieve(value, bitflags)
        serialized = bitflags.anybits?(FLAG_SERIALIZED)
        if serialized
          begin
            serializer.load(value)
          rescue StandardError
            raise UnmarshalError, 'Unable to unmarshal value'
          end
        else
          value
        end
      end

      def serializer
        @serialization_options[:serializer]
      end

      def serialize_value(value)
        serializer.dump(value)
      rescue Timeout::Error => e
        raise e
      rescue StandardError => e
        # Serializing can throw several different types of generic Ruby exceptions.
        # Convert to a specific exception so we can special case it higher up the stack.
        exc = Dalli::MarshalError.new(e.message)
        exc.set_backtrace e.backtrace
        raise exc
      end

      private

      # Defaults to client-level option, unless a request-level override is provided
      def serialize_value?(req_options)
        return !raw_by_default? unless req_options && !req_options[:raw].nil?

        !req_options[:raw]
      end

      def raw_by_default?
        @serialization_options[:raw]
      end
    end
  end
end
