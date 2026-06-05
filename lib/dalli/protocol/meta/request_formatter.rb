# frozen_string_literal: false

module Dalli
  module Protocol
    class Meta
      ##
      # Class that encapsulates logic for formatting meta protocol requests
      # to memcached.
      ##
      class RequestFormatter
        # Since these are string construction methods, we're going to disable these
        # Rubocop directives.  We really can't make this construction much simpler,
        # and introducing an intermediate object seems like overkill.
        #
        # rubocop:disable Metrics/CyclomaticComplexity
        # rubocop:disable Metrics/ParameterLists
        # rubocop:disable Metrics/PerceivedComplexity
        def self.meta_get(key:, value: true, return_cas: false, ttl: nil, base64: false, quiet: false,
                          meta_flags: nil, p_token: nil, l_token: nil)
          cmd = "mg #{key}"
          cmd << ' v f' if value
          cmd << ' c' if return_cas
          cmd << ' b' if base64
          cmd << " T#{ttl}" if ttl
          cmd << " #{meta_flags.join(' ')}" if meta_flags && !meta_flags.empty?
          cmd << routing_tokens(p_token: p_token, l_token: l_token)
          cmd << ' k q s' if quiet # Return the key in the response if quiet
          cmd + TERMINATOR
        end

        def self.meta_set(key:, value:, bitflags: nil, cas: nil, ttl: nil, mode: :set, base64: false, quiet: false,
                          p_token: nil, l_token: nil)
          cmd = "ms #{key} #{value.bytesize}"
          cmd << ' c' if !quiet && !%i[append prepend].include?(mode)
          cmd << ' b' if base64
          cmd << " F#{bitflags}" if bitflags
          cmd << cas_string(cas) if cas && cas != 0
          cmd << " T#{ttl}" if ttl
          cmd << " M#{mode_to_token(mode)}"
          cmd << ' q' if quiet
          cmd << routing_tokens(p_token: p_token, l_token: l_token)
          cmd << TERMINATOR
        end

        def self.meta_delete(key:, cas: nil, ttl: nil, base64: false, quiet: false, p_token: nil, l_token: nil,
                             invalidate: false, tombstone_ttl: nil, drop_value: false)
          raise ArgumentError, 'tombstone_ttl requires invalidate: true' if tombstone_ttl && !invalidate

          cmd = "md #{key}"
          cmd << ' b' if base64
          cmd << cas_string(cas)
          cmd << " T#{ttl}" if ttl
          cmd << ' q' if quiet
          cmd << ' I' if invalidate
          cmd << " T#{Integer(tombstone_ttl)}" if tombstone_ttl
          cmd << ' x' if drop_value
          cmd << routing_tokens(p_token: p_token, l_token: l_token)
          cmd + TERMINATOR
        end

        def self.meta_arithmetic(key:, delta:, initial:, incr: true, cas: nil, ttl: nil, base64: false, quiet: false,
                                 p_token: nil, l_token: nil)
          cmd = "ma #{key} v"
          cmd << ' b' if base64
          cmd << " D#{delta}" if delta
          cmd << " J#{initial}" if initial
          # Always set a TTL if an initial value is specified
          cmd << " N#{ttl || 0}" if ttl || initial
          cmd << cas_string(cas)
          cmd << ' q' if quiet
          cmd << " M#{incr ? 'I' : 'D'}"
          cmd << routing_tokens(p_token: p_token, l_token: l_token)
          cmd + TERMINATOR
        end

        # Builds the wire-format suffix for opaque routing tokens (P and L).
        #
        # Empty / nil tokens are treated as no-ops. CRLF and null bytes are
        # rejected with `ArgumentError` to prevent the token from being used
        # as a wire-protocol injection vector (e.g. `"foo\r\nflush_all\r\n"`
        # would otherwise be parsed as a second command by memcached or any
        # intermediate proxy/LB).
        def self.routing_tokens(p_token: nil, l_token: nil)
          p_token = nil if p_token.respond_to?(:empty?) && p_token.empty?
          l_token = nil if l_token.respond_to?(:empty?) && l_token.empty?
          validate_routing_token!('p_token', p_token)
          validate_routing_token!('l_token', l_token)
          return '' unless p_token || l_token

          s = +''
          s << " P#{p_token}" if p_token
          s << " L#{l_token}" if l_token
          s
        end

        # Disallowed bytes: CR, LF, NUL. Any of these embedded in a routing
        # token would let the caller inject a second wire-protocol command
        # (e.g. `"foo\r\nflush_all\r\n"`).
        #
        # Despite intuition, `match?` with a literal regex is ~2.3x faster
        # than `s.include?("\r") || s.include?("\n") || s.include?("\0")`
        # in microbenchmarks for short clean tokens (the hot path). Ruby's
        # Regexp engine fuses short character classes into a single C-level
        # scan, while the include? chain walks the string up to three times.
        ROUTING_TOKEN_FORBIDDEN = /[\r\n\0]/
        private_constant :ROUTING_TOKEN_FORBIDDEN

        def self.validate_routing_token!(name, value)
          return if value.nil?
          raise ArgumentError, "#{name} must be a String, got #{value.class}" unless value.is_a?(String)
          raise ArgumentError, "#{name} must not contain CRLF or null bytes" if value.match?(ROUTING_TOKEN_FORBIDDEN)
        end
        # rubocop:enable Metrics/CyclomaticComplexity
        # rubocop:enable Metrics/ParameterLists
        # rubocop:enable Metrics/PerceivedComplexity

        def self.meta_noop
          "mn#{TERMINATOR}"
        end

        def self.version
          "version#{TERMINATOR}"
        end

        def self.flush(delay: nil, quiet: false)
          cmd = +'flush_all'
          cmd << " #{parse_to_64_bit_int(delay, 0)}" if delay
          cmd << ' noreply' if quiet
          cmd + TERMINATOR
        end

        def self.stats(arg = nil)
          cmd = +'stats'
          cmd << " #{arg}" if arg
          cmd + TERMINATOR
        end

        def self.mode_to_token(mode)
          case mode
          when :add
            'E'
          when :replace
            'R'
          when :append
            'A'
          when :prepend
            'P'
          else
            'S'
          end
        end

        def self.cas_string(cas)
          cas = parse_to_64_bit_int(cas, nil)
          cas.nil? || cas.zero? ? '' : " C#{cas}"
        end

        def self.parse_to_64_bit_int(val, default)
          val.nil? ? nil : Integer(val)
        rescue ArgumentError
          # Sanitize to default if it isn't parsable as an integer
          default
        end
      end
    end
  end
end
