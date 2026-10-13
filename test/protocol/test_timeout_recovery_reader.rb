# frozen_string_literal: true

require_relative '../helper'
require 'objspace'

describe Dalli::Protocol::TimeoutRecoveryReader do
  def with_reader(timeout: 0.05)
    sock, peer = UNIXSocket.pair
    yield Dalli::Protocol::TimeoutRecoveryReader.new(sock, timeout), peer, sock
  ensure
    sock&.close
    peer&.close unless peer&.closed?
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

  it 'keeps its buffer at the chunk size after a large read' do
    value = SecureRandom.random_bytes(1_000_000)
    with_reader do |reader, peer|
      writer = write_in_background(peer, value, "\r\n")

      assert_equal value, reader.read(value.bytesize)
      assert_equal "\r\n", reader.gets("\r\n")
      writer.join
      buffer = reader.instance_variable_get(:@buffer)

      assert_operator ObjectSpace.memsize_of(buffer), :<=, Dalli::Protocol::TimeoutRecoveryReader::CHUNK_SIZE * 2
    end
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
