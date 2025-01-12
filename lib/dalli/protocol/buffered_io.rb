# frozen_string_literal: true

require 'socket'
require 'timeout'
require_relative 'connection_manager'

module Dalli
  module Protocol
    ##
    # Manages the buffer for responses from memcached.
    ##
    class BufferedIO
      ENCODING = Encoding::BINARY
      RETURN_ENCODING = Encoding::UTF_8
      TERMINATOR = "\r\n".b.freeze
      TERMINATOR_SIZE = TERMINATOR.bytesize
      DEFAULT_CHUNK_SIZE = 1024 * 8
      DEFAULT_SOCKET_TIMEOUT = ConnectionManager::DEFAULTS[:socket_timeout]

      def initialize(io, chunk_size = nil, timeout = nil)
        @io = io
        @buffer = +''
        @offset = 0
        @chunk_size = chunk_size || DEFAULT_CHUNK_SIZE
        @timeout = timeout || DEFAULT_SOCKET_TIMEOUT # seconds
      end

      # Reads line from io and the buffer, value does not include the terminator
      def read_line
        puts 'read_line' if $DEBUG
        fill_buffer(false) if @offset >= @buffer.bytesize
        until (terminator_index = @buffer.index(TERMINATOR, @offset))
          fill_buffer(false)
        end

        terminator_index += TERMINATOR_SIZE
        line = @buffer.byteslice(@offset, terminator_index - @offset)
        puts "line size: #{line.bytesize}" if $DEBUG
        @offset = terminator_index
        puts "line: #{line[0..100]}" if $DEBUG
        puts "line encoding: #{line.encoding}" if $DEBUG
        puts "line with encoding: #{line[0..100].force_encoding(RETURN_ENCODING)}" if $DEBUG
        line.force_encoding(RETURN_ENCODING)
      end

      # Reads the exact number of bytes from the buffer
      def read(size)
        puts "read #{size}" if $DEBUG
        needed = size - (@buffer.bytesize - @offset)
        fill_buffer(true, needed) if needed.positive?

        str = @buffer.byteslice(@offset, size)
        puts "str size: #{str.bytesize}" if $DEBUG
        @offset += size
        puts "str: #{str[0..100]}" if $DEBUG
        puts "str with encoding: #{str[0..100].encoding}" if $DEBUG
        puts "str with encoding: #{str[0..101].force_encoding(RETURN_ENCODING)}" if $DEBUG
        str.force_encoding(RETURN_ENCODING)
      end

      def write(str)
        puts "write #{str.bytesize}" if $DEBUG
        remaining = str.bytesize

        loop do
          bytes = @io.write_nonblock(str, exception: false)

          case bytes
          when Integer
            remaining -= bytes
            return if remaining <= 0

            str = str.byteslice(bytes..-1)
          when :wait_writable
            raise Timeout::Error unless @io.wait_writable(@timeout)
          else
            raise SystemCallError, 'Unhandled write_nonblock return value'
          end
        end
      end

      private

      # rubocop:disable Metrics/AbcSize
      # rubocop:disable Metrics/CyclomaticComplexity
      # rubocop:disable Metrics/PerceivedComplexity
      # rubocop:disable Metrics/MethodLength
      def fill_buffer(force_size, size = @chunk_size)
        puts "fill_buffer force=#{force_size}, chunk_size=#{size}" if $DEBUG
        remaining = size
        buffer_size = @buffer.bytesize
        start = @offset - buffer_size
        buffer_is_empty = start >= 0

        loop do
          bytes = if buffer_is_empty
                    @io.read_nonblock([remaining, @chunk_size].max, @buffer, exception: false)
                  else
                    @io.read_nonblock([remaining, @chunk_size].max, exception: false)
                  end

          case bytes
          when String
            puts "STRING size #{bytes.bytesize}" if $DEBUG
            if buffer_is_empty
              @offset = start
              @buffer if @buffer.encoding != ENCODING
            else
              @buffer << bytes
            end
            remaining -= bytes.bytesize

            return if !force_size || remaining <= 0
          when :wait_readable
            puts 'wait_readable' if $DEBUG
            @offset -= buffer_size if buffer_is_empty && @buffer.empty?

            raise Timeout::Error unless @io.wait_readable(@timeout)
          when nil
            puts 'nil' if $DEBUG
            raise EOFError
          else
            puts 'else' if $DEBUG
            raise SystemCallError, 'Unhandled read_nonblock return value'
          end
        end
      ensure
        puts 'end of fill_buffer' if $DEBUG
      end
      # rubocop:enable Metrics/AbcSize
      # rubocop:enable Metrics/CyclomaticComplexity
      # rubocop:enable Metrics/PerceivedComplexity
      # rubocop:enable Metrics/MethodLength
    end
  end
end
