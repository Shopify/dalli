# frozen_string_literal: true

require_relative '../helper'

# Stand-in for Async::Stop: the real class inherits from Exception (not
# StandardError) so that blanket `rescue StandardError` clauses don't
# accidentally swallow scheduler-driven fiber cancellation.  We reproduce that
# hierarchy locally to avoid pulling the `async` gem just for the signal class.
# rubocop:disable Lint/InheritException
class FiberCancellation < Exception; end
# rubocop:enable Lint/InheritException

describe 'fiber concurrency' do
  # `Async::Stop < Exception` but NOT `< StandardError`, so when the scheduler
  # cancels a fiber parked on a blocking read inside a Dalli request, the
  # `rescue StandardError` branch in `Base#request` is bypassed entirely.
  # Without an `ensure` clause, `close` is never called and `abort_request!`
  # never runs — the connection is left with `@request_in_progress == true`
  # and any partial response bytes remain on the wire for a subsequent caller
  # to misread.
  #
  # `Base#request` gates its `ensure` on `$ERROR_INFO` so that pipelined_get's
  # intentional request_in_progress=true happy path is not torn down — this
  # test proves the cleanup fires on abnormal exit for non-StandardError
  # exceptions.
  #
  # NOTE: this test uses the default `threadsafe: true` config.  The yield-
  # based interleave race (one fiber parking on IO while another enters
  # `request`) is separately protected by `Dalli::Threadsafe`, whose
  # `Monitor.synchronize` is fiber-aware under MRI and raises
  # `ThreadError: deadlock` rather than corrupting shared state.
  it 'does not leave the connection dirty when a non-StandardError aborts a request' do
    memcached_persistent do |dc|
      dc.flush
      dc.set('a', 'val_a')

      conn_mgr = dc.send(:ring).servers.first.instance_variable_get(:@connection_manager)

      close_count = 0
      orig_close = conn_mgr.method(:close)
      conn_mgr.define_singleton_method(:close) do
        close_count += 1
        orig_close.call
      end

      # Simulate Async::Stop raised into the fiber while it's parked on the
      # response read — i.e. the scheduler cancelling mid-request.
      conn_mgr.define_singleton_method(:read_line) do
        raise FiberCancellation, 'simulated fiber cancellation'
      end

      assert_raises(FiberCancellation) { dc.get('a') }

      refute_predicate conn_mgr, :request_in_progress?,
                       'connection left with @request_in_progress == true after non-StandardError abort'
      assert_operator close_count, :>=, 1,
                      "expected close to run during non-StandardError abort, got close_count=#{close_count}"
    end
  end

  # A second Async::Stop landing on the fiber while it is already inside the
  # `ensure` cleanup from the first one would interrupt `@sock.close`.
  # `ConnectionManager#close` rescues only StandardError around the socket
  # close, so a non-StandardError escaping `@sock.close` leaves `@sock`
  # non-nil and skips `abort_request!` — the client is returned to the pool
  # with `@request_in_progress == true` and a half-closed socket.
  it 'cleans up when @sock.close itself is interrupted by a second non-StandardError' do
    memcached_persistent do |dc|
      dc.flush
      dc.set('a', 'val_a') # warms up the connection so @sock exists

      conn_mgr = dc.send(:ring).servers.first.instance_variable_get(:@connection_manager)
      sock = conn_mgr.instance_variable_get(:@sock)

      refute_nil sock, 'precondition: socket should be connected after set'

      # Second cancellation: fires when close() reaches @sock.close.
      sock.define_singleton_method(:close) do
        raise FiberCancellation, 'simulated second fiber cancellation during @sock.close'
      end

      # First cancellation: fires when the request parks on a read.
      conn_mgr.define_singleton_method(:read_line) do
        raise FiberCancellation, 'simulated first fiber cancellation'
      end

      assert_raises(FiberCancellation) { dc.get('a') }

      refute_predicate conn_mgr, :request_in_progress?,
                       'double-exception in ensure left @request_in_progress == true'
      refute_predicate conn_mgr, :connected?,
                       'double-exception in ensure left @sock non-nil'
    end
  end
end
