# frozen_string_literal: true

require_relative '../helper'

describe 'Pipelined Delete' do
  it 'supports pipelined delete' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      # Set up some test data
      dc.set('a', 'foo')
      dc.set('b', 123)
      dc.set('c', %w[a b c])
      dc.set('d', 'test')
      dc.set('e', 'value')

      # Verify all keys exist
      resp = dc.get_multi(%w[a b c d e f])

      assert_equal(5, resp.size)
      assert_equal('foo', resp['a'])
      assert_equal(123, resp['b'])

      # Delete multiple keys (3 exist, 1 doesn't)
      result = dc.delete_multi(%w[a b c f])

      # Returns count of actually deleted keys
      assert_equal(3, result)

      # Verify keys were deleted
      resp = dc.get_multi(%w[a b c d e f])

      assert_equal(2, resp.size)
      assert_nil(resp['a'])
      assert_nil(resp['b'])
      assert_nil(resp['c'])
      assert_equal('test', resp['d'])
      assert_equal('value', resp['e'])
      assert_nil(resp['f'])
    end
  end

  it 'supports pipelined delete for single key' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      dc.set('a', 'foo')
      dc.set('b', 'bar')

      # Delete single key using multi method
      result = dc.delete_multi(['a'])

      assert_equal(1, result)

      # Verify only 'a' was deleted
      resp = dc.get_multi(%w[a b])

      assert_nil(resp['a'])
      assert_equal('bar', resp['b'])
    end
  end

  it 'handles empty key array' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      result = dc.delete_multi([])

      assert_equal(0, result)
    end
  end

  it 'handles non-existent keys' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      result = dc.delete_multi(%w[x y z])

      assert_equal(0, result)
      assert_nil(dc.get('x'))
      assert_nil(dc.get('y'))
      assert_nil(dc.get('z'))
    end
  end

  it 'supports pipelined delete with keys containing Unicode' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      # Set keys with Unicode
      dc.set('a', 'foo')
      dc.set('ƒ©åÍÎ', 'unicode_value')
      dc.set('b', 'bar')

      # Delete including Unicode key
      keys_to_delete = ['a', 'ƒ©åÍÎ']
      result = dc.delete_multi(keys_to_delete)

      assert_equal(2, result)

      # Check results - Unicode keys get encoded
      encoded_key = Dalli::Protocol::Meta::KeyRegularizer.encode('ƒ©åÍÎ')[0]

      # Verify deletion
      resp = dc.get_multi(['a', 'ƒ©åÍÎ', 'b'])

      assert_nil(resp['a'])
      assert_nil(resp[encoded_key])
      assert_equal('bar', resp['b'])
    end
  end

  it 'raises network errors during pipelined delete' do
    toxi_memcached_persistent(19_997, '', { down_retry_delay: 0 }) do |dc|
      dc.close
      dc.flush

      # Set up test data
      dc.set('a', 'foo')
      dc.set('b', 123)
      dc.set('c', 'test')

      # Verify keys exist
      resp = dc.get_multi(%w[a b c])

      assert_equal(3, resp.size)

      # Attempt delete with network down
      Toxiproxy[/dalli_memcached/].down do
        assert_raises Dalli::NetworkError do
          dc.delete_multi(%w[a b c])
        end
      end

      # Verify keys still exist after failed delete
      resp = dc.get_multi(%w[a b c])

      assert_equal(3, resp.size)
      assert_equal('foo', resp['a'])
      assert_equal(123, resp['b'])
      assert_equal('test', resp['c'])

      # Successful delete after network recovery
      result = dc.delete_multi(%w[a b])

      assert_equal(2, result)

      # Verify deletion
      resp = dc.get_multi(%w[a b c])

      assert_equal(1, resp.size)
      assert_nil(resp['a'])
      assert_nil(resp['b'])
      assert_equal('test', resp['c'])
    end
  end

  it 'performs large batch delete efficiently' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      # Set up 1000 keys
      keys = []
      dc.multi do
        1000.times do |idx|
          key = "key_#{idx}"
          dc.set(key, idx)
          keys << key
        end
      end

      # Verify all keys were set
      result = dc.get_multi(keys)

      assert_equal(1000, result.size)
      assert_equal(50, result['key_50'])

      # Delete first 500 keys
      keys_to_delete = keys[0...500]
      result = dc.delete_multi(keys_to_delete)

      assert_equal(500, result)

      # Verify remaining keys
      result = dc.get_multi(keys)

      assert_equal(500, result.size)
      assert_nil(result['key_0'])
      assert_nil(result['key_499'])
      assert_equal(500, result['key_500'])
      assert_equal(999, result['key_999'])
    end
  end

  it 'works with namespace option' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'test:namespace')
      dc.close
      dc.flush

      # Set keys with namespace
      dc.set('a', 'foo')
      dc.set('b', 'bar')
      dc.set('c', 'baz')

      # Delete with namespace
      result = dc.delete_multi(%w[a c])

      assert_equal(2, result)

      # Verify deletion
      resp = dc.get_multi(%w[a b c])

      assert_nil(resp['a'])
      assert_equal('bar', resp['b'])
      assert_nil(resp['c'])
    end
  end

  it 'handles mixed success and failure gracefully' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      # Set only some keys
      dc.set('exists1', 'value1')
      dc.set('exists2', 'value2')

      # Try to delete mix of existing and non-existing keys
      result = dc.delete_multi(%w[exists1 notexist1 exists2 notexist2])

      # Only 2 keys actually existed
      assert_equal(2, result)

      # Verify state
      resp = dc.get_multi(%w[exists1 notexist1 exists2 notexist2])

      assert_empty(resp)
    end
  end
end
