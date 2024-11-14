# frozen_string_literal: true

require 'socket'
require 'timeout'

module Dalli
  module Protocol
    ##
    # Manages the buffer for responses from memcached.
    ##
    class BufferedReader
      ENCODING = Encoding::BINARY
      TERMINATOR = "\r\n".b.freeze
      TERMINATOR_SIZE = TERMINATOR.bytesize

      attr_reader :buffer

      def initialize(io, chunk_size = nil, timeout = nil)
        @io = io
        @buffer = +''
        @offset = 0
        @chunk_size = chunk_size || (1024 * 8)
        @timeout = timeout || 1 # seconds
      end

      # Reads line from io and the buffer, value does not include the terminator
      def read_line
        fill_buffer(false) if @offset >= @buffer.bytesize
        until (terminator_index = @buffer.index(TERMINATOR, @offset))
          fill_buffer(false)
        end

        line = @buffer.byteslice(@offset, terminator_index - @offset)
        @offset = terminator_index + TERMINATOR_SIZE
        line.force_encoding(Encoding::UTF_8)
      end

      def read(size)
        size += TERMINATOR_SIZE
        needed = size - (@buffer.bytesize - @offset)
        fill_buffer(true, needed) if needed.positive?

        str = @buffer.byteslice(@offset, size - TERMINATOR_SIZE)
        @offset += size
        str.force_encoding(Encoding::UTF_8)
      end

      # rubocop:disable Metrics/AbcSize
      # rubocop:disable Metrics/CyclomaticComplexity
      # rubocop:disable Metrics/PerceivedComplexity
      # rubocop:disable Metrics/MethodLength
      def fill_buffer(force_size, size = @chunk_size)
        remaining = size
        buffer_size = @buffer.bytesize
        start = @offset - buffer_size
        buffer_is_empty = start >= 0
        current_timeout = @timeout.to_f

        loop do
          start_time = Time.now
          bytes = if buffer_is_empty
                    @io.read_nonblock([remaining, @chunk_size].max, @buffer, exception: false)
                  else
                    @io.read_nonblock([remaining, @chunk_size].max, exception: false)
                  end

          case bytes
          when :wait_readable
            @offset -= buffer_size if buffer_is_empty && @buffer.empty?

            raise Timeout::Error unless @io.wait_readable(current_timeout)
          when :wait_writable
            raise Dalli::DalliError, 'Unreachable code path wait_writable'
          when nil
            raise EOFError
          else
            if buffer_is_empty
              @offset = start
              buffer_is_empty = false
              @buffer.force_encoding(ENCODING) if @buffer.encoding != ENCODING
            else
              @buffer << bytes.force_encoding(ENCODING)
            end
            remaining -= bytes.bytesize

            return if !force_size || remaining <= 0
          end

          current_timeout = [current_timeout - (Time.now - start_time), 0.0].max
          raise Timeout::Error if current_timeout <= 0
        end
      end
      # rubocop:enable Metrics/AbcSize
      # rubocop:enable Metrics/CyclomaticComplexity
      # rubocop:enable Metrics/PerceivedComplexity
      # rubocop:enable Metrics/MethodLength
    end
  end
end
