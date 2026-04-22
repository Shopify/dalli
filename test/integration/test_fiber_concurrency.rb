# frozen_string_literal: true

require_relative '../helper'
require 'fiber'

# Dalli's ConnectionManager tracks an in-flight request via @request_in_progress
# and wraps every request with `start_request!` / `finish_request!`.  When two
# fibers share a single Dalli::Client and a fiber-aware scheduler yields one
# fiber mid-request (e.g. a blocking socket read), a second fiber entering
# `Base#request` sees `@request_in_progress == true` inside
# `ConnectionManager#confirm_ready!` and tears down the shared socket.  The
# paused fiber's response is then either lost, read off a reconnected socket,
# or surfaces as a StandardError that gets rescued in `Base#request` — which
# calls `close` again.
#
# This test simulates what Fiber.scheduler does on a blocking read by
# yielding once inside the response-line read that Dalli issues after writing
# a get request.  It is intentionally failing today: the assertion is that no
# close occurs during interleaved fiber gets on a shared connection.
describe 'fiber concurrency' do
  it 'does not tear down the shared connection when two fibers interleave gets' do
    memcached_persistent(21_345, '', { socket_timeout: 0.5 }) do |dc|
      dc.flush
      dc.set('a', 'val_a')
      dc.set('b', 'val_b')

      conn_mgr = dc.send(:ring).servers.first.instance_variable_get(:@connection_manager)

      close_count = 0
      orig_close = conn_mgr.method(:close)
      conn_mgr.define_singleton_method(:close) do
        close_count += 1
        orig_close.call
      end

      # Fiber.scheduler parks a fiber on blocking IO; we simulate that here by
      # yielding once inside the response-line read.  Thread.current[] is
      # fiber-scoped in MRI, so each fiber controls its own yield flag.
      orig_read_line = conn_mgr.method(:read_line)
      conn_mgr.define_singleton_method(:read_line) do
        if Thread.current[:dalli_fiber_yield_next_read]
          Thread.current[:dalli_fiber_yield_next_read] = false
          Fiber.yield
        end
        orig_read_line.call
      end

      results = {}
      errors = {}

      make_fiber = lambda do |key|
        Fiber.new do
          Thread.current[:dalli_fiber_yield_next_read] = true
          results[key] = dc.get(key)
        rescue StandardError => e
          errors[key] = e
        end
      end

      fa = make_fiber.call('a')
      fb = make_fiber.call('b')

      # fa writes, yields inside the response read.  fb enters `request` and
      # confirm_ready! observes the in-progress flag — the bug path.
      fa.resume
      fb.resume
      fa.resume while fa.alive?
      fb.resume while fb.alive?

      assert_equal 0, close_count,
                   "connection was torn down #{close_count} time(s) by interleaved fiber gets; " \
                   "errors=#{errors.inspect} results=#{results.inspect}"
      assert_empty errors, "fibers raised unexpectedly: #{errors}"
      assert_equal 'val_a', results['a']
      assert_equal 'val_b', results['b']
    end
  end
end
