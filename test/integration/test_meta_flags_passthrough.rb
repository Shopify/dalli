# frozen_string_literal: true

require_relative '../helper'

# Integration tests for the `meta_flags:` passthrough on storage, delete,
# arithmetic, and pipelined commands.
#
# These tests assert two things:
#   1. The flags are accepted by memcached (it ignores P and L per spec).
#   2. The operation still completes successfully (the response is parsed
#      correctly even when meta_flags are present).
#
# We use the proxy-reserved 'P' and 'L' flags from the memcached meta protocol,
# which are explicitly designed to be ignored by memcached itself and consumed
# by intermediate proxies/routers.
PROXY_FLAGS = ['Proute=test-region', 'Lhint=local'].freeze

describe 'meta_flags passthrough' do
  describe 'set / add / replace' do
    it 'accepts meta_flags and stores the value' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('mfk', 'val1', nil, meta_flags: PROXY_FLAGS))
        assert_equal 'val1', dc.get('mfk')

        assert op_addset_succeeds(dc.replace('mfk', 'val2', nil, meta_flags: PROXY_FLAGS))
        assert_equal 'val2', dc.get('mfk')

        # add against an existing key should fail (without raising), even with flags
        refute dc.add('mfk', 'val3', nil, meta_flags: PROXY_FLAGS)

        dc.delete('mfk')

        assert op_addset_succeeds(dc.add('mfk', 'val4', nil, meta_flags: PROXY_FLAGS))
        assert_equal 'val4', dc.get('mfk')
      end
    end
  end

  describe 'delete' do
    it 'accepts meta_flags and deletes the value' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('mfk', 'val')

        assert dc.delete('mfk', meta_flags: PROXY_FLAGS)
        assert_nil dc.get('mfk')
      end
    end
  end

  describe 'incr / decr' do
    it 'accepts meta_flags and adjusts the counter' do
      memcached_persistent do |dc|
        dc.flush

        assert_equal 5, dc.incr('counter', 1, 60, 5, meta_flags: PROXY_FLAGS)
        assert_equal 6, dc.incr('counter', 1, 60, 5, meta_flags: PROXY_FLAGS)
        assert_equal 4, dc.decr('counter', 2, 60, 5, meta_flags: PROXY_FLAGS)
      end
    end
  end

  describe 'append / prepend' do
    it 'accepts meta_flags and updates the value' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('mfk', 'middle', 0, raw: true)

        assert dc.append('mfk', '_end', meta_flags: PROXY_FLAGS)
        assert dc.prepend('mfk', 'start_', meta_flags: PROXY_FLAGS)

        assert_equal 'start_middle_end', dc.get('mfk', raw: true)
      end
    end
  end

  describe 'get_multi' do
    it 'applies the same meta_flags to every key in the pipeline' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('a', '1')
        dc.set('b', '2')
        dc.set('c', '3')

        results = dc.get_multi('a', 'b', 'c', meta_flags: PROXY_FLAGS)

        assert_equal({ 'a' => '1', 'b' => '2', 'c' => '3' }, results)
      end
    end

    it 'works with the block form of get_multi (multi-server pipelined path)' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('a', '1')
        dc.set('b', '2')

        seen = {}
        dc.get_multi('a', 'b', meta_flags: PROXY_FLAGS) { |k, v| seen[k] = v }

        assert_equal({ 'a' => '1', 'b' => '2' }, seen)
      end
    end

    it 'works without meta_flags (backward-compat sanity)' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('a', '1')
        dc.set('b', '2')

        assert_equal({ 'a' => '1', 'b' => '2' }, dc.get_multi('a', 'b'))
      end
    end
  end

  describe 'set_multi (pipelined)' do
    it 'applies meta_flags to every entry in the pipeline' do
      memcached_persistent do |dc|
        dc.flush

        dc.set_multi({ 'a' => '1', 'b' => '2', 'c' => '3' }, 60, meta_flags: PROXY_FLAGS)

        assert_equal({ 'a' => '1', 'b' => '2', 'c' => '3' }, dc.get_multi('a', 'b', 'c'))
      end
    end
  end

  describe 'delete_multi (pipelined)' do
    it 'applies meta_flags to every entry in the pipeline' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('a', '1')
        dc.set('b', '2')
        dc.set('c', '3')

        deleted = dc.delete_multi(%w[a b c], meta_flags: PROXY_FLAGS)

        assert_equal 3, deleted
        assert_nil dc.get('a')
        assert_nil dc.get('b')
        assert_nil dc.get('c')
      end
    end
  end

  describe 'empty meta_flags array' do
    # An empty array must behave identically to omitting the option entirely:
    # no extra wire bytes, no protocol errors, normal responses. This guards
    # against accidental regressions of the trailing-space bug that was fixed
    # alongside this passthrough work.
    it 'is a no-op for set / delete / incr / decr / get / get_multi' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('mfk', 'val', nil, meta_flags: []))
        assert_equal 'val', dc.get('mfk', meta_flags: [])

        assert_equal 5, dc.incr('cnt', 1, 60, 5, meta_flags: [])
        assert_equal 6, dc.incr('cnt', 1, 60, 5, meta_flags: [])
        assert_equal 4, dc.decr('cnt', 2, 60, 5, meta_flags: [])

        dc.set('a', '1')
        dc.set('b', '2')

        assert_equal({ 'a' => '1', 'b' => '2' }, dc.get_multi('a', 'b', meta_flags: []))

        assert dc.delete('mfk', meta_flags: [])
        assert_nil dc.get('mfk')
      end
    end

    it 'is a no-op for set_multi and delete_multi' do
      memcached_persistent do |dc|
        dc.flush

        dc.set_multi({ 'a' => '1', 'b' => '2' }, 60, meta_flags: [])

        assert_equal({ 'a' => '1', 'b' => '2' }, dc.get_multi('a', 'b'))

        assert_equal 2, dc.delete_multi(%w[a b], meta_flags: [])
        assert_empty dc.get_multi('a', 'b')
      end
    end
  end

  describe 'unknown / invalid flag' do
    # Sanity: prove that memcached actually does the parsing and that an
    # invalid letter is rejected. This locks in the fact that P/L being
    # ignored is a real protocol feature and not just incidental tolerance.
    it 'raises Dalli::DalliError when an unknown meta flag is sent' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('mfk', 'val')

        assert_raises Dalli::DalliError do
          dc.get('mfk', meta_flags: ['Yroute=nope'])
        end
      end
    end
  end
end
