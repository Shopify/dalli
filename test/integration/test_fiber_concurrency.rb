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
end
