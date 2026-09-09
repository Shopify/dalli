# frozen_string_literal: true

require_relative '../helper'

# IO#read(maxlen) on a blocking socket returns exactly maxlen bytes unless the
# stream hits EOF, where it returns a shorter (non-nil) buffer or nil. This
# models that contract deterministically: one queued result per read call, so we
# can exercise the full read and the premature-EOF cases without a real socket.
class ContractReadSocket
  attr_reader :read_calls

  def initialize(*chunks)
    @chunks = chunks
    @read_calls = []
    @closed = false
  end

  def read(count)
    @read_calls << count
    @chunks.shift
  end

  def close
    @closed = true
  end

  def closed?
    @closed
  end
end

describe Dalli::Protocol::ConnectionManager do
  def connection_manager_with_socket(socket)
    manager = Dalli::Protocol::ConnectionManager.new(
      'localhost',
      11_211,
      :tcp,
      socket_failure_delay: nil,
      socket_max_failures: 2
    )
    manager.instance_variable_set(:@sock, socket)
    manager
  end

  it 'keeps the socket open after an ordinary completed request' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    manager.start_request!
    manager.finish_request!

    refute_predicate manager, :request_in_progress?
    assert_predicate manager, :connected?
    refute_predicate socket, :closed?
  end

  it 'discards a rejected response connection when the request completes, without retrying' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    manager.start_request!
    manager.discard_after_request!

    assert_predicate manager, :request_in_progress?
    refute_predicate socket, :closed?

    manager.finish_request!

    assert_predicate socket, :closed?
    refute_predicate manager, :connected?
    refute_predicate manager, :request_in_progress?
    assert_empty socket.read_calls

    replacement = ContractReadSocket.new
    manager.instance_variable_set(:@sock, replacement)
    manager.start_request!
    manager.finish_request!

    refute_predicate replacement, :closed?, 'discard state must not leak to the next connection'
  end

  it 'clears pending discard state when a request is closed before completing' do
    manager = connection_manager_with_socket(ContractReadSocket.new)
    manager.start_request!
    manager.discard_after_request!
    manager.close

    replacement = ContractReadSocket.new
    manager.instance_variable_set(:@sock, replacement)
    manager.start_request!
    manager.finish_request!

    refute_predicate replacement, :closed?
    refute_predicate manager, :request_in_progress?
  end

  it 'returns the full buffer from a single read, binary-safe' do
    socket = ContractReadSocket.new("a\x00\xFFz".b)
    manager = connection_manager_with_socket(socket)

    result = manager.read_exact(4)

    assert_equal "a\x00\xFFz".b, result
    assert_equal Encoding::BINARY, result.encoding
    assert_equal [4], socket.read_calls
    # #read shares the same fill semantics (both delegate to read_bytes).
    assert_equal 'xyz', connection_manager_with_socket(ContractReadSocket.new('xyz')).read(3)
  end

  it 'raises and closes the dirty socket on a premature EOF (nil)' do
    socket = ContractReadSocket.new(nil)
    manager = connection_manager_with_socket(socket)

    assert_raises(Dalli::RetryableNetworkError) { manager.read_exact(5) }
    assert_predicate socket, :closed?
    refute_predicate manager, :connected?
  end

  it 'raises and closes the dirty socket on a short read (EOF mid-response)' do
    socket = ContractReadSocket.new('abc')
    manager = connection_manager_with_socket(socket)

    assert_raises(Dalli::RetryableNetworkError) { manager.read(5) }
    assert_predicate socket, :closed?
  end
end
