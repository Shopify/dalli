# frozen_string_literal: true

require_relative '../helper'
require 'objspace'
require_relative '../utils/ssl_socket_pair'

describe Dalli::Protocol::TimeoutRecoveryReader do
  def with_reader(timeout: 0.05)
    sock, peer = UNIXSocket.pair
    yield Dalli::Protocol::TimeoutRecoveryReader.new(sock, timeout), peer, sock
  ensure
    sock&.close
    peer&.close unless peer&.closed?
  end

  def elapsed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  end

  # Default UNIX socket buffers (8 KB on macOS) cap each read regardless of the reader.
  def enlarge_buffers(sock, peer)
    peer.setsockopt(Socket::SOL_SOCKET, Socket::SO_SNDBUF, 1 << 20)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_RCVBUF, 1 << 20)
  end

  # Records what each read_nonblock on sock returns: :data for bytes, otherwise the result itself.
  def record_reads(sock)
    results = []
    recorder = Module.new do
      define_method(:read_nonblock) do |*args, **kwargs|
        super(*args, **kwargs).tap { |result| results << (result.is_a?(String) ? :data : result) }
      end
    end
    sock.singleton_class.prepend(recorder)
    results
  end

  def write_in_background(peer, *chunks)
    Thread.new do
      chunks.each { |chunk| peer.write(chunk) }
      peer.close
    end
  end

  it 'reads lines and values that cross chunk boundaries' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    lines = Array.new(2_000) { |i| "VA #{i} k#{'x' * (i % 97)}\r\n" }
    value = SecureRandom.random_bytes((chunk * 3) + 17)
    with_reader do |reader, peer|
      writer = write_in_background(peer, lines.join, value, "\r\nEN\r\n")

      assert_equal lines, Array.new(lines.size) { reader.gets("\r\n") }
      assert_equal value, reader.read(value.bytesize)
      assert_equal "\r\n", reader.read(2)
      assert_equal "EN\r\n", reader.gets("\r\n")
      assert_nil reader.gets("\r\n")
      writer.join
    end
  end

  it 'matches IO#gets and IO#read encodings and EOF results' do
    input = "VA 3\r\nabc\r\npartial"
    expected = with_reader do |_, peer, sock|
      write_in_background(peer, input).join
      [sock.gets("\r\n"), sock.read(3), sock.gets("\n"), sock.gets("\r\n"), sock.gets("\r\n"), sock.read(4)]
    end
    actual = with_reader do |reader, peer|
      write_in_background(peer, input).join
      [reader.gets("\r\n"), reader.read(3), reader.gets("\n"), reader.gets("\r\n"), reader.gets("\r\n"), reader.read(4)]
    end

    assert_equal expected, actual
    assert_equal expected.compact.map(&:encoding), actual.compact.map(&:encoding)
  end

  it 'returns short reads at EOF' do
    with_reader do |reader, peer|
      write_in_background(peer, 'abc').join

      assert_equal 'abc', reader.read(10)
      assert_nil reader.read(10)
    end
  end

  it 'raises IO::TimeoutError when nothing arrives before the timeout' do
    with_reader(timeout: 0.05) do |reader|
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      assert_raises(IO::TimeoutError) { reader.gets("\r\n") }
      assert_operator Process.clock_gettime(Process::CLOCK_MONOTONIC) - started, :>=, 0.05
    end
  end

  it 'reads data that is ready when the wait reports a timeout' do
    with_reader do |reader, peer, sock|
      sock.define_singleton_method(:wait_readable) do |timeout|
        return super(timeout) if timeout.zero?

        peer.write("HD\r\n")
        nil
      end

      assert_equal "HD\r\n", reader.gets("\r\n")
    end
  end

  it 'reads data that is ready when the poll after a timed-out wait misses it' do
    with_reader do |reader, peer, sock|
      # A zero-timeout poll can report nothing without checking the socket while the thread has a
      # pending interrupt, such as one from Thread#wakeup or a queue push.
      sock.define_singleton_method(:wait_readable) do |timeout|
        peer.write("HD\r\n") if timeout.positive?
        nil
      end

      assert_equal "HD\r\n", reader.gets("\r\n")
    end
  end

  it 'keeps its buffer at the large read size after a large read' do
    value = SecureRandom.random_bytes(1_000_000)
    with_reader do |reader, peer|
      writer = write_in_background(peer, value, "\r\n")

      assert_equal value, reader.read(value.bytesize)
      assert_equal "\r\n", reader.gets("\r\n")
      writer.join
      buffer = reader.instance_variable_get(:@buffer)

      assert_operator ObjectSpace.memsize_of(buffer), :<=, Dalli::Protocol::TimeoutRecoveryReader::LARGE_READ_SIZE * 2
    end
  end

  it 'returns a value that ends at the end of a read without sharing the buffer' do
    with_reader do |reader, peer|
      peer.write("VA 1000 k\r\n", 'v' * 1000)
      sleep(0.01)

      assert_equal "VA 1000 k\r\n", reader.gets("\r\n")
      value = reader.read(1000)
      peer.write("\r\n")

      assert_equal "\r\n", reader.read(2)
      refute_includes ObjectSpace.dump(value), '"shared":true'
    end
  end

  it 'reads a reply of many small values in a few socket reads' do
    values = Array.new(40) { |i| SecureRandom.random_bytes(4096 + i) }
    reply = values.map { |value| "VA #{value.bytesize} k\r\n#{value}\r\n" }.join.b << "MN\r\n"
    with_reader do |reader, peer, sock|
      reads = record_reads(sock)
      enlarge_buffers(sock, peer)
      write_in_background(peer, reply).join

      values.each do |value|
        assert_equal "VA #{value.bytesize} k\r\n", reader.gets("\r\n")
        assert_equal value, reader.read(value.bytesize)
        assert_equal "\r\n", reader.read(2)
      end

      assert_equal "MN\r\n", reader.gets("\r\n")
      # 8 KB reads would take 21.
      assert_operator reads.size, :<=, 5
    end
  end

  it 'waits for each expected reply before its first read' do
    with_reader do |reader, peer, sock|
      reads = record_reads(sock)
      2.times do
        reader.expect_reply
        writer = Thread.new do
          sleep(0.01)
          peer.write("HD\r\n")
        end

        assert_equal "HD\r\n", reader.gets("\r\n")
        writer.join
      end

      assert_equal %i[data data], reads
    end
  end

  it 'reads a large value in a few socket reads' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    value = SecureRandom.random_bytes((chunk * 40) + 3)
    with_reader do |reader, peer, sock|
      reads = record_reads(sock)
      enlarge_buffers(sock, peer)
      half = value.bytesize / 2
      write_in_background(peer, "VA #{value.bytesize}\r\n", value.byteslice(0, half),
                          value.byteslice(half..), "\r\nEN\r\n").join

      assert_equal "VA #{value.bytesize}\r\n", reader.gets("\r\n")
      assert_equal value, reader.read(value.bytesize)
      assert_operator reads.size, :<, 20
      assert_equal "\r\n", reader.read(2)
      assert_equal "EN\r\n", reader.gets("\r\n")
      assert_nil reader.gets("\r\n")
    end
  end

  it 'returns the bytes received when EOF cuts a large value short' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    value = SecureRandom.random_bytes(chunk * 3)
    with_reader do |reader, peer|
      writer = write_in_background(peer, value)

      assert_equal value, reader.read(chunk * 5)
      assert_nil reader.read(chunk * 5)
      writer.join
    end
  end

  it 'keeps reading a large value when a wait in the middle of it reports a timeout while data is ready' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    value = SecureRandom.random_bytes(chunk * 8)
    with_reader(timeout: 0.05) do |reader, peer, sock|
      enlarge_buffers(sock, peer)
      peer.write(value.byteslice(0, chunk * 3))
      timed_out = []
      reads = record_reads(sock)
      # A scheduler that resumes the reader after the deadline reports a timeout even though the
      # rest of the value has arrived by then.
      sock.define_singleton_method(:wait_readable) do |timeout|
        return super(timeout) if timeout.zero? || reads.empty?

        timed_out << timeout
        peer.write(value.byteslice((chunk * 3)..)) if timed_out.one?
        nil
      end

      assert_equal value, reader.read(value.bytesize)
      assert_equal [0.05], timed_out
    end
  end

  it 'fails within one timeout once a large value stops arriving' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    with_reader(timeout: 0.2) do |reader, peer, sock|
      enlarge_buffers(sock, peer)
      peer.write(SecureRandom.random_bytes(chunk * 3))
      duration = elapsed { Timeout.timeout(5) { assert_raises(IO::TimeoutError) { reader.read(chunk * 8) } } }

      # One wait for the timeout, then a poll that does not wait.
      assert_operator duration, :>=, 0.2
      assert_operator duration, :<, 0.3
    end
  end

  it 'gives each wait a full timeout while a large value arrives in pauses' do
    chunk = Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE
    parts = Array.new(6) { SecureRandom.random_bytes(chunk * 2) }
    with_reader(timeout: 0.1) do |reader, peer|
      writer = Thread.new do
        parts.each do |part|
          peer.write(part)
          sleep(0.03)
        end
      end
      duration = elapsed { assert_equal parts.join, reader.read(parts.sum(&:bytesize)) }

      assert_operator duration, :>, 0.1
      writer.join
    end
  end

  it 'returns nil at EOF instead of replaying earlier bytes when a TLS peer closes' do
    sock, peer = SSLSocketPair.open
    reader = Dalli::Protocol::TimeoutRecoveryReader.new(sock, 0.05)
    peer.write("HD\r\n")
    peer.close

    assert_equal "HD\r\n", reader.gets("\r\n")
    assert_nil reader.gets("\r\n")
    assert_nil reader.read(1)
  ensure
    sock&.close
  end

  it 'allocates one string per buffered line' do
    with_reader do |reader, peer|
      peer.write("HD\r\n" * 101)
      reader.gets("\r\n")
      before = GC.stat(:total_allocated_objects)
      100.times { reader.gets("\r\n") }

      assert_operator GC.stat(:total_allocated_objects) - before, :<=, 105
    end
  end
end
