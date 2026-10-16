# frozen_string_literal: true

module Dalli
  module Protocol
    ##
    # Buffered response reader that waits with explicit deadlines and, when a wait
    # expires, polls the socket once outside any fiber scheduler before failing.
    ##
    class TimeoutRecoveryReader
      CHUNK_SIZE = 8192
      # Most a single large-value read asks for. Bounds the per-connection scratch
      # buffer at 64 KiB while cutting a 2 MB value from ~257 reads to ~35.
      LARGE_READ_SIZE = 65_536

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
        remaining = count - available
        # Large values bypass the 8 KB line buffer: each read asks the socket for at
        # most what is still owed, so a value costs a few syscalls rather than one per
        # CHUNK_SIZE bytes plus a copy out of @buffer, and never reads past its end.
        return append_directly(data, remaining) if remaining >= CHUNK_SIZE

        append_from_socket(data, remaining)
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

      def append_directly(data, remaining)
        while remaining.positive?
          chunk = if data.empty?
                    read_partial(remaining, data)
                  else
                    read_partial([remaining, LARGE_READ_SIZE].min, scratch)
                  end
          return data.empty? ? nil : data unless chunk

          data << chunk unless chunk.equal?(data)
          remaining -= chunk.bytesize
        end
        data
      end

      def scratch
        @scratch ||= String.new(encoding: Encoding::BINARY)
      end

      # Reads at most maxlen bytes into outbuf, waiting while the socket has none; nil at EOF.
      def read_partial(maxlen, outbuf)
        last_try = false
        while (result = @sock.read_nonblock(maxlen, outbuf, exception: false)).is_a?(Symbol)
          raise IO::TimeoutError, "IO timeout after #{@timeout}s" if last_try

          last_try = !wait_for(result)
        end
        result
      end

      # read_nonblock overwrites the buffer, so unconsumed bytes are set aside and restored
      # after the read; @offset only moves once the read returns. At EOF a socket empties the
      # buffer but OpenSSL leaves it untouched, so it is cleared here to avoid replaying old bytes.
      def fill
        partial = @buffer.byteslice(@offset, @buffer.bytesize - @offset) if pending?
        filled = !read_partial(CHUNK_SIZE, @buffer).nil?
        @buffer.clear unless filled
        @offset = 0
        @buffer.prepend(partial) if partial
        filled
      end

      # Waits on :wait_readable or :wait_writable, which name the socket method to call. After a
      # wait that times out, polls once outside the fiber scheduler. Returns false when that poll
      # finds nothing too, and the caller then reads once more before failing: a zero-timeout poll
      # can report nothing without checking the socket while the thread has a pending interrupt.
      def wait_for(event)
        return true if @sock.public_send(event, @timeout)

        Fiber.blocking { @sock.public_send(event, 0) } ? true : false
      end
    end
  end
end
