# frozen_string_literal: true

require_relative '../helper'

# IO#read(maxlen) on a blocking socket returns exactly maxlen bytes unless the
# stream hits EOF, where it returns a shorter (non-nil) buffer or nil. This
# models that contract deterministically: one queued result per read call, so we
# can exercise the full read and the premature-EOF cases without a real socket.
class ContractReadSocket
  attr_reader :read_calls
  attr_accessor :sync

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

  it 'seeds one connection-local generator and uses it for compact request opaques' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    random = Random.new(123)
    expected = random.dup
    Random.stub(:new, random) do
      manager.stub(:memcached_socket, socket) { manager.establish_connection }
    end

    tokens = Random.stub(:new, -> { raise 'must not reseed per request' }) do
      Array.new(100) { manager.generate_opaque }
    end

    assert_equal Array.new(100) { expected.urlsafe_base64(8, false) }, tokens
    assert_equal 100, tokens.uniq.size
    assert(tokens.all? { |token| /\A[A-Za-z0-9_-]{11}\z/.match?(token) })
  end

  it 'does not share opaque generator state between connections' do
    sockets = Array.new(2) { ContractReadSocket.new }
    managers = sockets.map { |socket| connection_manager_with_socket(socket) }
    managers.zip(sockets).each do |manager, socket|
      manager.stub(:memcached_socket, socket) { manager.establish_connection }
    end
    first_random, second_random = managers.map { |manager| manager.instance_variable_get(:@opaque_random) }
    expected = second_random.dup.urlsafe_base64(8, false)
    managers.first.generate_opaque

    refute_same first_random, second_random
    assert_equal expected, managers.last.generate_opaque
  end

  it 'replaces the opaque generator on reconnect and releases it on close' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    manager.stub(:memcached_socket, socket) { manager.establish_connection }
    first_random = manager.instance_variable_get(:@opaque_random)
    manager.close

    assert_nil manager.instance_variable_get(:@opaque_random)

    replacement = ContractReadSocket.new
    manager.stub(:memcached_socket, replacement) { manager.establish_connection }
    second_random = manager.instance_variable_get(:@opaque_random)

    refute_same first_random, second_random
    refute_equal first_random.seed, second_random.seed
    assert_match(/\A[A-Za-z0-9_-]{11}\z/, manager.generate_opaque)
  end

  it 'does not charge discarded responses to the network failure budget' do
    socket = ContractReadSocket.new
    manager = connection_manager_with_socket(socket)
    manager.start_request!
    manager.discard_after_request!

    assert_predicate manager, :request_in_progress?
    refute_predicate socket, :closed?

    manager.finish_request!

    assert_predicate socket, :closed?
    refute_predicate manager, :request_in_progress?
    assert_empty socket.read_calls

    manager.instance_variable_set(:@sock, ContractReadSocket.new(nil))

    assert_raises(Dalli::RetryableNetworkError) { manager.read(5) }
    assert_predicate manager, :reconnect_down_server?
  end

  [false, true].each do |in_rescue|
    it "records string failures without caller exception context (caller rescue: #{in_rescue})" do
      manager = connection_manager_with_socket(ContractReadSocket.new)
      manager.options[:socket_max_failures] = 1
      reason = 'EOF in read_line'

      error = assert_raises(Dalli::NetworkError) do
        if in_rescue
          begin
            raise ArgumentError, 'unrelated caller failure'
          rescue ArgumentError
            manager.error_on_request!(reason)
          end
        else
          manager.error_on_request!(reason)
        end
      end

      assert_equal "localhost:11211 is down: #{reason}", error.message
      refute_predicate manager, :connected?
      refute_predicate manager, :reconnect_down_server?
    end
  end

  it 'records the supplied exception class even when that exception was never raised' do
    manager = connection_manager_with_socket(ContractReadSocket.new)
    manager.options[:socket_max_failures] = 1
    failure = EOFError.new('truncated response')

    error = assert_raises(Dalli::NetworkError) { manager.error_on_request!(failure) }

    assert_instance_of Dalli::NetworkError, error
    assert_equal 'localhost:11211 is down: EOFError truncated response', error.message
    refute_predicate manager, :connected?
  end

  it 'clears pending discard state when a request is closed before completing' do
    manager = connection_manager_with_socket(ContractReadSocket.new)
    manager.start_request!
    manager.discard_after_request!
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

    assert_raises(RuntimeError) { manager.discard_after_request! }
    assert_raises(Dalli::RetryableNetworkError) { manager.error_on_request!('first real failure') }
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
