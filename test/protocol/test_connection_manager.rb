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

  it 'shares failure accounting between discarded responses and network errors' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    manager.start_request!
    manager.discard_after_request!('opaque mismatch')

    assert_predicate manager, :request_in_progress?
    refute_predicate socket, :closed?

    manager.finish_request!

    assert_predicate socket, :closed?
    refute_predicate manager, :request_in_progress?
    assert_empty socket.read_calls

    manager.instance_variable_set(:@sock, ContractReadSocket.new(nil))
    manager.up!
    error = assert_raises(Dalli::NetworkError) { manager.read(5) }

    assert_instance_of Dalli::NetworkError, error
    refute_predicate manager, :reconnect_down_server?
  end

  it 'marks the server down without raising when a discarded response reaches the failure limit' do
    manager = connection_manager_with_socket(ContractReadSocket.new)

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first failure') }

    manager.instance_variable_set(:@sock, ContractReadSocket.new)
    manager.up!
    manager.start_request!
    manager.discard_after_request!('opaque mismatch')
    manager.finish_request!

    refute_predicate manager, :connected?
    refute_predicate manager, :request_in_progress?
    refute_predicate manager, :reconnect_down_server?

    error = assert_raises(Dalli::NetworkError) { manager.raise_down_error }

    assert_includes error.message, 'opaque mismatch'
  end

  it 'clears pending discard state when a request is closed before completing' do
    manager = connection_manager_with_socket(ContractReadSocket.new)
    manager.start_request!
    manager.discard_after_request!('opaque mismatch')
    manager.close

    replacement = ContractReadSocket.new
    manager.instance_variable_set(:@sock, replacement)
    manager.up!
    manager.start_request!
    manager.finish_request!

    refute_predicate replacement, :closed?
    assert_predicate manager, :reconnect_down_server?
  end

  it 'requires an active request before recording a discard' do
    manager = connection_manager_with_socket(ContractReadSocket.new)

    assert_raises(RuntimeError) { manager.discard_after_request!('opaque mismatch') }
    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first real failure') }
  end

  it 'preserves failures across reconnects so repeated stream errors mark the server down' do
    socket = ContractReadSocket.new(nil)
    manager = connection_manager_with_socket(socket)

    assert_raises(Dalli::RetryableNetworkError) { manager.read(5) }
    assert_predicate socket, :closed?

    replacement = ContractReadSocket.new(nil)
    manager.instance_variable_set(:@sock, replacement)
    manager.up! # The version handshake succeeded, not the failed operation.

    error = assert_raises(Dalli::NetworkError) { manager.read(5) }

    assert_instance_of Dalli::NetworkError, error
    assert_predicate replacement, :closed?
    refute_predicate manager, :reconnect_down_server?
  end

  it 'resets the failure budget after a successful request' do
    manager = connection_manager_with_socket(ContractReadSocket.new)

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first failure') }

    manager.instance_variable_set(:@sock, ContractReadSocket.new)
    manager.up!
    manager.start_request!
    manager.finish_request!

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('independent failure') }
  end

  it 'allows a fresh failure budget when a down server is probed again' do
    manager = connection_manager_with_socket(ContractReadSocket.new)
    manager.options[:down_retry_delay] = 0

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first failure') }
    assert_raises(Dalli::NetworkError) { manager.error_on_request!('second failure') }
    assert_predicate manager, :reconnect_down_server?

    manager.instance_variable_set(:@sock, ContractReadSocket.new)
    manager.up!

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('failure after cooldown') }
  end

  it 'retains a string-valued stream error in the final down error' do
    manager = connection_manager_with_socket(ContractReadSocket.new)

    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first failure') }

    error = assert_raises(Dalli::NetworkError) { manager.error_on_request!('opaque mismatch') }

    assert_includes error.message, 'opaque mismatch'
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
