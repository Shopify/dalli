# frozen_string_literal: true

module Dalli
  module Protocol
    ##
    # Buffered response reader that waits with explicit deadlines and, when a wait
    # expires, polls the socket once outside any fiber scheduler before failing.
    ##
    class TimeoutRecoveryReader
      CHUNK_SIZE = 8192

      def initialize(sock, timeout)
        @sock = sock
        @timeout = timeout
        @buffer = String.new(capacity: CHUNK_SIZE, encoding: Encoding::BINARY)
        @offset = 0
      end

      def pending?
        @offset < @buffer.bytesize
      end

      def gets(separator)
        until (index = @buffer.byteindex(separator, @offset))
          next if fill

          return pending? ? consume(@buffer.bytesize - @offset) : nil
        end
        consume(index + separator.bytesize - @offset)
      end

      def read(count)
        available = @buffer.bytesize - @offset
        return consume(count) if available >= count

        data = String.new(capacity: count, encoding: Encoding::BINARY)
        data << @buffer.byteslice(@offset, available) if available.positive?
        @offset = @buffer.bytesize
        append_from_socket(data, count - available)
      end

      private

      def consume(length)
        data = @buffer.byteslice(@offset, length)
        @offset += length
        data
      end

      def append_from_socket(data, remaining)
        while remaining.positive?
          return data.empty? ? nil : data unless fill

          taken = [remaining, @buffer.bytesize].min
          data << (taken == @buffer.bytesize ? @buffer : @buffer.byteslice(0, taken))
          @offset = taken
          remaining -= taken
        end
        data
      end

      # read_nonblock overwrites the buffer, so unconsumed bytes are set aside and restored
      # after the read; @offset only moves once the read returns.
      def fill
        partial = @buffer.byteslice(@offset, @buffer.bytesize - @offset) if pending?
        filled = read_chunk
        @offset = 0
        if partial
          filled ? @buffer.prepend(partial) : @buffer.replace(partial)
        end
        filled
      end

      # Waits on :wait_readable or :wait_writable, which name the socket method to call.
      def read_chunk
        while (result = @sock.read_nonblock(CHUNK_SIZE, @buffer, exception: false)).is_a?(Symbol)
          wait_for(result)
        end
        !result.nil?
      end

      def wait_for(event)
        return if @sock.public_send(event, @timeout)
        return if Fiber.blocking { @sock.public_send(event, 0) }

        raise IO::TimeoutError, "IO timeout after #{@timeout}s"
      end
    end
  end
end
