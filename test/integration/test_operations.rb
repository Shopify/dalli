# frozen_string_literal: true

require_relative '../helper'
require 'openssl'
require 'securerandom'

describe 'operations' do
  describe 'get' do
    it 'returns the value on a hit' do
      memcached_persistent do |dc|
        dc.flush

        val1 = '1234567890' * 999_999
        dc.set('a', val1)
        val2 = dc.get('a')

        assert_equal val1, val2

        assert op_addset_succeeds(dc.set('a', nil))
        assert_nil dc.get('a')
      end
    end

    it 'returns the value on a hit when using raw single server optimized get' do
      memcached_persistent do |_, port|
        dc = Dalli::Client.new("localhost:#{port}", namespace: 'some:namspace', raw: true)
        dc.close
        dc.flush

        val1 = '1234567890' * 50
        dc.set('a', val1)
        val2 = dc.get('a')

        assert_equal val1, val2
      end
    end

    it 'set_multi respects namespaces when using raw single server' do
      memcached_persistent do |_, port|
        dc = Dalli::Client.new("localhost:#{port}", namespace: 'some:namspace', raw: true)
        dc.close
        dc.flush

        pairs = { 'paira' => 'vala', 'pairb' => 'valb', 'pairc' => 'valc' }
        dc.set_multi(pairs, 5)

        assert_equal pairs, dc.get_multi(pairs.keys)
      end
    end

    it 'set_multi respects namespaces on multiple servers' do
      memcached_persistent do |dc, _port|
        pairs = { 'paira' => 'vala', 'pairb' => 'valb', 'pairc' => 'valc' }
        dc.set_multi(pairs, 5)

        assert_equal pairs, dc.get_multi(pairs.keys)
      end
    end

    it 'return the value that include TERMINATOR on a hit' do
      memcached_persistent do |dc|
        dc.flush

        val1 = "12345#{Dalli::Protocol::Meta::TERMINATOR}67890"
        dc.set('a', val1)
        val2 = dc.get('a')

        assert_equal val1, val2

        assert op_addset_succeeds(dc.set('a', nil))
        assert_nil dc.get('a')
      end
    end

    it 'return the value and the meta flags' do
      memcached_persistent do |dc|
        dc.flush

        val1 = 'meta'
        dc.set('meta_key', val1)
        val2, meta_flags = dc.get('meta_key', meta_flags: %i[l h t])

        assert_equal val1, val2
        # not yet hit, and last accessed 0 from set
        assert_equal({ c: 0, l: 0, h: false, t: -1, bitflag: nil }.sort, meta_flags.sort)

        sleep 1 # we can't simulate time in memcached so we need to sleep
        # ensure hit true and last accessed 1
        val2, meta_flags = dc.get('meta_key', meta_flags: %i[l h t])

        assert_equal val1, val2
        assert_equal({ c: 0, l: 1, h: true, t: -1, bitflag: nil }.sort, meta_flags.sort)

        assert op_addset_succeeds(dc.set('meta_key', nil))
        assert_nil dc.get('meta_key')
      end
    end

    it 'returns nil on a miss' do
      memcached_persistent do |dc|
        assert_nil dc.get('notexist')
      end
    end

    it 'get handles socket timeouts errors' do
      toxi_memcached_persistent(MemcachedManager::TOXIPROXY_UPSTREAM_PORT, '', { socket_timeout: 1 }) do |dc|
        dc.close
        dc.flush

        resp = dc.get_multi(%w[a b c d e f])

        assert_empty(resp)

        dc.set('a', 'foo')
        dc.set('b', 123)
        dc.set('c', %w[a b c])

        keys = %w[a b c d e f]
        Toxiproxy[/dalli_memcached/].downstream(:latency, latency: 2000).apply do
          keys.each do |key|
            assert_raises Dalli::RingError do
              dc.get(key)
            end
          end
        end
      end
    end

    it 'get handles network errors' do
      toxi_memcached_persistent do |dc|
        dc.close
        dc.flush

        resp = dc.get_multi(%w[a b c d e f])

        assert_empty(resp)

        dc.set('a', 'foo')
        dc.set('b', 123)
        dc.set('c', %w[a b c])

        keys = %w[a b c d e f]
        Toxiproxy[/dalli_memcached/].down do
          keys.each do |key|
            assert_raises Dalli::RingError do
              dc.get(key)
            end
          end
        end
      end
    end

    it 'returns nil on a miss with meta flags' do
      memcached_persistent do |dc|
        val2, meta_flags = dc.get('notexist', meta_flags: %i[l h t])

        assert_nil val2
        assert_empty(meta_flags)
      end
    end

    # This is historical, as the 'Not found' value was
    # treated as a special case at one time
    it 'allows "Not found" as value' do
      memcached_persistent do |dc|
        dc.set('key1', 'Not found')

        assert_equal 'Not found', dc.get('key1')
      end
    end
  end

  describe 'gat' do
    it 'returns the value and touches on a hit' do
      memcached_persistent do |dc|
        dc.flush
        dc.set 'key', 'value'

        assert_equal 'value', dc.gat('key', 10)
        assert_equal 'value', dc.gat('key')
      end
    end

    it 'returns nil on a miss' do
      memcached_persistent do |dc|
        dc.flush

        assert_nil dc.gat('notexist', 10)
      end
    end

    it 'return the value and the meta flags' do
      memcached_persistent do |dc|
        dc.flush

        val1 = 'meta'
        dc.set('meta_key', val1)
        dc.gat('meta_key', 10, meta_flags: %i[l h t])
        # ensure hit true and last accessed 0
        response = dc.gat('meta_key', 10, meta_flags: %i[l h t])
        val2 = response.first
        meta_flags = response.last

        assert_equal val1, val2
        assert_equal({ c: 0, l: 0, h: true, t: 10, bitflag: nil }, meta_flags)

        assert op_addset_succeeds(dc.set('meta_key', nil))
        assert_nil dc.get('meta_key')
      end
    end
  end

  describe 'touch' do
    it 'returns true on a hit' do
      memcached_persistent do |dc|
        dc.flush
        dc.set 'key', 'value'

        assert dc.touch('key', 10)
        assert dc.touch('key')
        assert_equal 'value', dc.get('key')
        assert_nil dc.touch('notexist')
      rescue Dalli::DalliError => e
        # This will happen when memcached is in lesser version than 1.4.8
        assert_equal 'Response error 129: Unknown command', e.message
      end
    end

    it 'returns nil on a miss' do
      memcached_persistent do |dc|
        dc.flush

        assert_nil dc.touch('notexist')
      end
    end
  end

  describe 'set' do
    it 'returns a CAS when the key exists and updates the value' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('key', 'value')

        assert op_replace_succeeds(dc.set('key', 'value2'))

        assert_equal 'value2', dc.get('key')
      end
    end

    it 'returns a CAS when no pre-existing value exists' do
      memcached_persistent do |dc|
        dc.flush

        assert op_replace_succeeds(dc.set('key', 'value2'))
        assert_equal 'value2', dc.get('key')
      end
    end
  end

  describe 'add' do
    it 'returns false when replacing an existing value and does not update the value' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('key', 'value')

        refute dc.add('key', 'value')

        assert_equal 'value', dc.get('key')
      end
    end

    it 'returns a CAS when no pre-existing value exists' do
      memcached_persistent do |dc|
        dc.flush

        assert op_replace_succeeds(dc.add('key', 'value2'))
      end
    end
  end

  describe 'replace' do
    it 'returns a CAS when the key exists and updates the value' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('key', 'value')

        assert op_replace_succeeds(dc.replace('key', 'value2'))

        assert_equal 'value2', dc.get('key')
      end
    end

    it 'returns false when no pre-existing value exists' do
      memcached_persistent do |dc|
        dc.flush

        refute dc.replace('key', 'value')
      end
    end
  end

  describe 'delete' do
    it 'returns true on a hit and deletes the entry' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('some_key', 'some_value')

        assert_equal 'some_value', dc.get('some_key')

        assert dc.delete('some_key')
        assert_nil dc.get('some_key')

        refute dc.delete('nonexist')
      end
    end

    it 'returns false on a miss' do
      memcached_persistent do |dc|
        dc.flush

        refute dc.delete('nonexist')
      end
    end
  end

  describe 'fetch' do
    it 'fetches pre-existing values' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('fetch_key', 'Not found')
        res = dc.fetch('fetch_key') { flunk 'fetch block called' }

        assert_equal 'Not found', res
      end
    end

    it 'supports with default values' do
      memcached_persistent do |dc|
        dc.flush

        expected = { 'blah' => 'blerg!' }
        executed = false
        value = dc.fetch('fetch_key') do
          executed = true
          expected
        end

        assert_equal expected, value
        assert executed

        executed = false
        value = dc.fetch('fetch_key') do
          executed = true
          expected
        end

        assert_equal expected, value
        refute executed
      end
    end

    it 'supports with falsey values' do
      memcached_persistent do |dc|
        dc.flush

        dc.set('fetch_key', false)
        res = dc.fetch('fetch_key') { flunk 'fetch block called' }

        refute res
      end
    end

    it 'supports with nil values when cache_nils: true' do
      memcached_persistent(21_345, '', cache_nils: true) do |dc|
        dc.flush

        dc.set('fetch_key', nil)
        res = dc.fetch('fetch_key') { flunk 'fetch block called' }

        assert_nil res
      end

      memcached_persistent(21_345, '', cache_nils: false) do |dc|
        dc.flush
        dc.set('fetch_key', nil)
        executed = false
        res = dc.fetch('fetch_key') do
          executed = true
          'bar'
        end

        assert_equal 'bar', res
        assert executed
      end
    end
  end

  describe 'incr/decr' do
    it 'supports incrementing and decrementing existing values' do
      memcached_persistent do |client|
        client.flush

        assert op_addset_succeeds(client.set('fakecounter', 0, 0, raw: true))
        assert_equal 1, client.incr('fakecounter', 1)
        assert_equal 2, client.incr('fakecounter', 1)
        assert_equal 3, client.incr('fakecounter', 1)
        assert_equal 1, client.decr('fakecounter', 2)
        assert_equal '1', client.get('fakecounter')
      end
    end

    it 'returns nil on a miss with no initial value' do
      memcached_persistent do |client|
        client.flush

        resp = client.incr('mycounter', 1)

        assert_nil resp

        resp = client.decr('mycounter', 1)

        assert_nil resp
      end
    end

    it 'enables setting an initial value with incr and subsequently incrementing/decrementing' do
      memcached_persistent do |client|
        client.flush

        resp = client.incr('mycounter', 1, 0, 2)

        assert_equal 2, resp
        resp = client.incr('mycounter', 1)

        assert_equal 3, resp

        resp = client.decr('mycounter', 2)

        assert_equal 1, resp
      end
    end

    it 'supports setting the initial value with decr and subsequently incrementing/decrementing' do
      memcached_persistent do |dc|
        dc.flush

        resp = dc.decr('counter', 100, 5, 0)

        assert_equal 0, resp

        resp = dc.decr('counter', 10)

        assert_equal 0, resp

        resp = dc.incr('counter', 10)

        assert_equal 10, resp

        current = 10
        100.times do |x|
          resp = dc.incr('counter', 10)

          assert_equal current + ((x + 1) * 10), resp
        end
      end
    end

    it 'supports 64-bit values' do
      memcached_persistent do |dc|
        dc.flush

        resp = dc.decr('10billion', 0, 5, 10)

        assert_equal 10, resp
        # go over the 32-bit mark to verify proper (un)packing
        resp = dc.incr('10billion', 10_000_000_000)

        assert_equal 10_000_000_010, resp

        resp = dc.decr('10billion', 1)

        assert_equal 10_000_000_009, resp

        resp = dc.decr('10billion', 0)

        assert_equal 10_000_000_009, resp

        resp = dc.incr('10billion', 0)

        assert_equal 10_000_000_009, resp

        resp = dc.decr('10billion', 9_999_999_999)

        assert_equal 10, resp

        resp = dc.incr('big', 100, 5, 0xFFFFFFFFFFFFFFFE)

        assert_equal 0xFFFFFFFFFFFFFFFE, resp
        resp = dc.incr('big', 1)

        assert_equal 0xFFFFFFFFFFFFFFFF, resp

        # rollover the 64-bit value, we'll get something undefined.
        resp = dc.incr('big', 1)

        refute_equal 0x10000000000000000, resp
        dc.reset
      end
    end
  end

  describe 'append/prepend' do
    it 'support the append and prepend operations' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('456', 'xyz', 0, raw: true))
        assert dc.prepend('456', '0')
        assert dc.append('456', '9')
        assert_equal '0xyz9', dc.get('456')

        refute dc.append('nonexist', 'abc')
        refute dc.prepend('nonexist', 'abc')
      end
    end
  end
end
