# frozen_string_literal: true

module Dalli
  module Protocol
    ##
    # Buffered response reader that waits with explicit deadlines and, when a wait
    # expires, polls the socket once outside any fiber scheduler before failing.
    ##
    class TimeoutRecoveryReader
      # Size of the first reads into the buffer.
      CHUNK_SIZE = 8192
      # Size of every read into the buffer once one CHUNK_SIZE read comes back full, that is once a
      # reply on this connection outgrows CHUNK_SIZE. A get_multi of 40 × 4 KB values then takes 3
      # reads instead of 22. The buffer stays at 64 KiB plus any partial line carried over a read.
      LARGE_READ_SIZE = 65_536
      # Separators for #gets. byteindex with a separator in another encoding than @buffer's
      # first scans @buffer's coderange, which every fill resets, so it costs a pass over
      # every chunk read; a binary separator skips that scan.
      LF = "\n".b.freeze
      CRLF = "\r\n".b.freeze

      def initialize(sock, timeout)
        @sock = sock
        @timeout = timeout
        @buffer = String.new(capacity: CHUNK_SIZE, encoding: Encoding::BINARY)
        @offset = 0
        @read_size = CHUNK_SIZE
        # Plain sockets wait before a reply's first read; see read_partial.
        @wait_before_reply = sock.is_a?(::BasicSocket)
        @reply_expected = true
      end

      # Called before a request is written, so the first read of its reply waits first.
      def expect_reply
        @reply_expected = true
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
        # A byteslice that runs to the end of @buffer shares its memory, and the next fill would then
        # copy the whole buffer before overwriting it. unpack1 copies just the bytes asked for.
        append(data, @buffer.unpack1('a*', offset: @offset)) if available.positive?
        @offset = @buffer.bytesize
        remaining = count - available
        while remaining.positive?
          return data.empty? ? nil : data unless fill

          taken = [remaining, @buffer.bytesize].min
          append(data, taken == @buffer.bytesize ? @buffer : @buffer.byteslice(0, taken))
          @offset = taken
          remaining -= taken
        end
        data
      end

      private

      def consume(length)
        data = @buffer.byteslice(@offset, length)
        @offset += length
        data
      end

      # String#<< scans the bytes it appends to work out the result's coderange. append_as_bytes
      # (Ruby 3.4+) skips that scan and, like <<, keeps the capacity data was created with.
      if String.method_defined?(:append_as_bytes)
        def append(data, bytes)
          data.append_as_bytes(bytes)
        end
      else
        def append(data, bytes)
          data << bytes
        end
      end

      # Reads at most maxlen bytes into outbuf, waiting while the socket has none; nil at EOF.
      #
      # A reply's first read comes right after its request is written, so on a plain socket it would
      # only return :wait_readable; the socket is waited on first instead, the same wait that read
      # would have led to. OpenSSL can hold decrypted bytes that the socket's readiness doesn't show,
      # so TLS sockets always read first.
      def read_partial(maxlen, outbuf)
        last_try = @reply_expected && @wait_before_reply && !wait_for(:wait_readable)
        @reply_expected = false
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
        filled = !read_partial(@read_size, @buffer).nil?
        @read_size = LARGE_READ_SIZE if @buffer.bytesize == @read_size
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
