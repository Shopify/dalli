# frozen_string_literal: true

require_relative '../helper'

# Integration tests for memcached tombstone support: the meta-protocol's
# `I` (mark stale), `T` (tombstone TTL on delete), and `x` (drop value)
# flags on `md`, plus the `X` response flag on `mg` exposed via
# Client#get_with_status.
#
# A tombstoned item lives briefly in a "stale" window where reads can tell
# a racing repopulator from a true miss — caller checks result.stale? vs
# result.miss?. Designed for high-concurrency invalidation scenarios where
# a hard delete would let an in-flight request rewrite a stale value over
# the deleted key.
describe 'tombstone (mark-stale) support' do
  describe 'Client#get_with_status return shape' do
    it 'returns a Dalli::CacheResult with hit? on a normal hit' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('tk', 'val'))

        result = dc.get_with_status('tk')

        assert_kind_of Dalli::CacheResult, result
        assert_equal 'val', result.value
        assert_predicate result, :hit?
        refute_predicate result, :miss?
        refute_predicate result, :stale?
      end
    end

    it 'returns miss? on a true miss (key never existed)' do
      memcached_persistent do |dc|
        dc.flush

        result = dc.get_with_status('absent')

        assert_kind_of Dalli::CacheResult, result
        assert_nil result.value
        assert_predicate result, :miss?
        refute_predicate result, :hit?
        refute_predicate result, :stale?
      end
    end

    it 'returns miss? after a regular (non-tombstone) delete' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('tk', 'val'))
        dc.delete('tk')

        result = dc.get_with_status('tk')

        assert_predicate result, :miss?
        refute_predicate result, :stale?
      end
    end

    it 'is frozen so callers cannot mutate the returned object' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tk', 'val')

        assert_predicate dc.get_with_status('tk'), :frozen?
      end
    end
  end

  describe 'delete with invalidate: true' do
    it 'leaves the item readable but marked stale' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('tk', 'preserved-val'))

        dc.delete('tk', invalidate: true)
        result = dc.get_with_status('tk')

        assert_predicate result, :stale?, 'expected X flag (stale) after invalidate'
        assert_predicate result, :hit?, 'invalidate without drop_value should leave value readable'
        refute_predicate result, :miss?
        assert_equal 'preserved-val', result.value
      end
    end

    it 'leaves an empty value when drop_value is also set' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('tk', 'should-be-dropped'))

        dc.delete('tk', invalidate: true, drop_value: true)
        result = dc.get_with_status('tk')

        assert_predicate result, :stale?
        refute_predicate result, :miss?
        # Value is dropped — empty string, not the original
        refute_equal 'should-be-dropped', result.value
      end
    end

    it 'transitions from stale? to miss? after tombstone_ttl elapses' do
      memcached_persistent do |dc|
        dc.flush

        assert op_addset_succeeds(dc.set('tk', 'val'))

        dc.delete('tk', invalidate: true, tombstone_ttl: 1, drop_value: true)

        # Within the tombstone window
        assert_predicate dc.get_with_status('tk'), :stale?

        # Past the tombstone window, the X flag should be gone
        sleep 2
        result = dc.get_with_status('tk')

        assert_predicate result, :miss?, 'tombstone should have expired into a true miss'
        refute_predicate result, :stale?
      end
    end

    it 'does not emit a tombstone for a plain delete' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tk', 'val')

        dc.delete('tk') # no kwargs — regular hard delete
        result = dc.get_with_status('tk')

        assert_predicate result, :miss?
        refute_predicate result, :stale?
      end
    end

    it 'is reachable via delete_cas with explicit cas' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tk', 'val')
        cas = dc.get_cas('tk').last

        dc.delete_cas('tk', cas, invalidate: true, tombstone_ttl: 30)

        assert_predicate dc.get_with_status('tk'), :stale?
      end
    end
  end

  describe 'delete_multi with invalidate' do
    it 'tombstones every key in the batch' do
      memcached_persistent do |dc|
        dc.flush
        keys = %w[tm-a tm-b tm-c]
        keys.each { |k| dc.set(k, "val-#{k}") }

        deleted = dc.delete_multi(keys, invalidate: true, tombstone_ttl: 30)

        assert_equal keys.length, deleted
        keys.each do |k|
          result = dc.get_with_status(k)

          assert_predicate result, :stale?, "expected #{k} to be stale after delete_multi(invalidate: true)"
          assert_equal "val-#{k}", result.value
        end
      end
    end

    it 'drops values across the batch when drop_value is set' do
      memcached_persistent do |dc|
        dc.flush
        keys = %w[tm-x tm-y]
        keys.each { |k| dc.set(k, 'orig') }

        dc.delete_multi(keys, invalidate: true, tombstone_ttl: 30, drop_value: true)

        keys.each do |k|
          result = dc.get_with_status(k)

          assert_predicate result, :stale?
          refute_equal 'orig', result.value
        end
      end
    end
  end

  describe 'argument validation' do
    it 'raises ArgumentError when tombstone_ttl is supplied without invalidate' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tk', 'val')

        assert_raises(ArgumentError) do
          dc.delete('tk', tombstone_ttl: 30)
        end
      end
    end
  end

  describe 'interaction with quiet block' do
    it 'allows tombstone delete inside quiet (delete is in ALLOWED_QUIET_OPS)' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tq', 'val')

        dc.quiet do
          dc.delete('tq', invalidate: true, tombstone_ttl: 30)
        end

        # Outside the quiet block, the tombstone should be visible
        assert_predicate dc.get_with_status('tq'), :stale?
      end
    end

    it 'raises NotPermittedMultiOpError when get_with_status is called inside quiet' do
      memcached_persistent do |dc|
        dc.flush
        dc.set('tq', 'val')

        assert_raises(Dalli::NotPermittedMultiOpError) do
          dc.quiet do
            dc.get_with_status('tq')
          end
        end
      end
    end
  end
end
