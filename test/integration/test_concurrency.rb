# frozen_string_literal: true

require_relative '../helper'

describe 'concurrent behavior' do
  it 'supports multithreaded access' do
    memcached_persistent do |cache|
      cache.flush
      workers = []

      cache.set('f', 'zzz')

      assert op_cas_succeeds(cache.cas('f') do |value|
        value << 'z'
      end)
      assert_equal 'zzzz', cache.get('f')

      # Have a bunch of threads perform a bunch of operations at the same time.
      # Verify the result of each operation to ensure the request and response
      # are not intermingled between threads.
      10.times do
        workers << Thread.new do
          100.times do
            cache.set('a', 9)
            cache.set('b', 11)
            cache.incr('cat', 10, 0, 10)
            cache.set('f', 'zzz')
            res = cache.cas('f') do |value|
              value << 'z'
            end

            refute_nil res
            refute cache.add('a', 11)
            assert_equal({ 'a' => 9, 'b' => 11 }, cache.get_multi(%w[a b]))
            inc = cache.incr('cat', 10)

            assert_equal 0, inc % 5
            cache.decr('cat', 5)

            assert_equal 11, cache.get('b')

            assert_equal %w[a b], cache.get_multi('a', 'b', 'c').keys.sort
          end
        end
      end

      workers.each(&:join)
      cache.flush
    end
  end

  it 'supports multithreaded access for single server optimized' do
    memcached_persistent do |_cache, port|
      # NOTE: we have a bug with set multi and a namespace  namespace: 'some:namspace' fails
      cache = Dalli::Client.new("localhost:#{port}", raw: true)
      cache.close
      cache.flush
      workers = []

      cache.set('f', 'zzz')

      assert op_cas_succeeds(cache.cas('f') do |value|
        value << 'z'
      end)
      assert_equal 'zzzz', cache.get('f')

      multi_keys = { 'ab' => 'vala', 'bb' => 'valb', 'cb' => 'valc' }

      # Have a bunch of threads perform a bunch of operations at the same time.
      # Verify the result of each operation to ensure the request and response
      # are not intermingled between threads.
      10.times do
        workers << Thread.new do
          100.times do
            cache.set('a', 9)
            cache.set('b', 11)
            cache.set('f', 'zzz')
            cache.set_multi(multi_keys, 10)
            res = cache.cas('f') do |value|
              value << 'z'
            end

            refute_nil res
            refute cache.add('a', 11)
            assert_equal({ 'a' => '9', 'b' => '11' }, cache.get_multi(%w[a b]))

            assert_equal '11', cache.get('b')
            assert_equal 'vala', cache.get('ab')

            assert_equal %w[a b], cache.get_multi('a', 'b', 'c').keys.sort
            assert_equal multi_keys, cache.get_multi(multi_keys.keys)
            cache.set_multi(multi_keys, 10)
          end
        end
      end

      workers.each(&:join)
      cache.flush
    end
  end

  # Have a bunch of threads perform a bunch of operations at the same time.
  # Verify the result of each operation to ensure the request and response
  # are not intermingled between forks.
  it 'supports multi process client usage for multiple servers' do
    memcached_persistent do |_cache, port|
      memcached_persistent do |_cache, port2|
        cache = Dalli::Client.new(["localhost:#{port}", "localhost:#{port2}"], socket_timeout: 0.1,
                                                                               socket_max_failures: 0,
                                                                               socket_failure_delay: 0.0,
                                                                               down_retry_delay: 0.0)
        cache.close
        cache.flush
        workers = []

        cache.set('f', 'zzz')

        assert op_cas_succeeds(cache.cas('f') do |value|
          value << 'z'
        end)
        assert_equal 'zzzz', cache.get('f')
        multi_keys = { 'ab' => 'vala', 'bb' => 'valb', 'cb' => 'valc', 'dd' => 'vald' }
        cache.set_multi(multi_keys, 10)

        10.times do
          cache.get_multi(multi_keys.keys)
          workers << Process.fork do
            # first request after forking will try to reconnect to the server, we need to ensure we hit both rings
            cache.set('ring1', 'work')
            cache.set('ring2', 'work')
            sleep(0.2)
            cache.set_multi(multi_keys, 10)
            100.times do
              cache.set('a', 9)
              cache.set('b', 11)
              cache.set('f', 'zzz')
              cache.set_multi(multi_keys, 10)
              res = cache.cas('f') do |value|
                value << 'z'
              end

              assert_equal multi_keys, cache.get_multi(multi_keys.keys)

              refute_nil res
              refute cache.add('a', 11)
              assert_equal({ 'a' => 9, 'b' => 11 }, cache.get_multi(%w[a b]))

              assert_equal 11, cache.get('b')

              assert_equal %w[a b], cache.get_multi('a', 'b', 'c').keys.sort
            end
          end
        end

        Process.wait
        sleep(1) # if we don't sleep between the two protocol tests, second fails on connection issues
      end
    end
  end

  it 'supports multi process client usage for single server' do
    memcached_persistent do |_cache, port|
      cache = Dalli::Client.new("localhost:#{port}", socket_timeout: 0.1, protocol: p,
                                                     socket_max_failures: 0,
                                                     socket_failure_delay: 0.0,
                                                     down_retry_delay: 0.0)
      cache.close
      cache.flush
      workers = []

      cache.set('f', 'zzz')

      assert op_cas_succeeds(cache.cas('f') do |value|
        value << 'z'
      end)
      assert_equal 'zzzz', cache.get('f')

      10.times do
        workers << Process.fork do
          # first request after forking will try to reconnect to the server, we need to ensure we hit both rings
          cache.set('ring1', 'work')
          sleep(0.2)
          10.times do
            cache.set('a', 9)
            cache.set('b', 11)
            cache.set('f', 'zzz')
            res = cache.cas('f') do |value|
              value << 'z'
            end

            refute_nil res
            refute cache.add('a', 11)
            assert_equal({ 'a' => 9, 'b' => 11 }, cache.get_multi(%w[a b]))

            assert_equal 11, cache.get('b')

            assert_equal %w[a b], cache.get_multi('a', 'b', 'c').keys.sort
          end
        end
      end

      Process.wait
      sleep(1) # if we don't sleep between the two protocol tests, second fails on connection issues
    end
  end

  it 'supports multi process client usage for single server raw optimized' do
    memcached_persistent do |_cache, port|
      cache = Dalli::Client.new("localhost:#{port}", raw: true, socket_timeout: 0.1, protocol: p,
                                                     socket_max_failures: 0,
                                                     socket_failure_delay: 0.0,
                                                     down_retry_delay: 0.0)
      cache.close
      cache.flush
      workers = []

      cache.set('f', 'zzz')

      assert op_cas_succeeds(cache.cas('f') do |value|
        value << 'z'
      end)
      assert_equal 'zzzz', cache.get('f')

      10.times do
        workers << Process.fork do
          # first request after forking will try to reconnect to the server, we need to ensure we hit both rings
          cache.set('ring1', 'work')
          sleep(0.2)
          10.times do
            cache.set('a', 9)
            cache.set('b', 11)
            cache.set('f', 'zzz')
            res = cache.cas('f') do |value|
              value << 'z'
            end

            refute_nil res
            refute cache.add('a', 11)
            assert_equal({ 'a' => '9', 'b' => '11' }, cache.get_multi(%w[a b]))

            assert_equal '11', cache.get('b')

            assert_equal %w[a b], cache.get_multi('a', 'b', 'c').keys.sort
          end
        end
      end

      Process.wait
      sleep(1) # if we don't sleep between the two protocol tests, second fails on connection issues
    end
  end
end
