# frozen_string_literal: true

require_relative '../helper'

describe 'Pipelined Get' do
  it 'supports pipelined get' do
    memcached_persistent do |dc|
      dc.close
      dc.flush
      resp = dc.get_multi(%w[a b c d e f])

      assert_empty(resp)

      dc.set('a', 'foo')
      dc.set('b', 123)
      dc.set('c', %w[a b c])

      # Invocation without block
      resp = dc.get_multi(%w[a b c d e f])
      expected_resp = { 'a' => 'foo', 'b' => 123, 'c' => %w[a b c] }

      assert_equal(expected_resp, resp)

      # Invocation with block
      dc.get_multi(%w[a b c d e f]) do |k, v|
        assert(expected_resp.key?(k) && expected_resp[k] == v)
        expected_resp.delete(k)
      end

      assert_empty expected_resp

      # Perform a big quiet set with 1000 elements.
      arr = []
      dc.multi do
        1000.times do |idx|
          dc.set idx, idx
          arr << idx
        end
      end

      # Retrieve the elements with a pipelined get
      result = dc.get_multi(arr)

      assert_equal(1000, result.size)
      assert_equal(50, result['50'])
    end
  end

  it 'supports pipelined get for single key' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      keys_to_query = ['a']

      resp = dc.get_multi(keys_to_query)

      assert_empty(resp)

      dc.set('a', 'foo')

      # Invocation without block
      resp = dc.get_multi(keys_to_query)
      expected_resp = { 'a' => 'foo' }

      assert_equal(expected_resp, resp)

      # Invocation with block
      dc.get_multi(keys_to_query) do |k, v|
        assert(expected_resp.key?(k) && expected_resp[k] == v)
        expected_resp.delete(k)
      end

      assert_empty expected_resp
    end
  end

  it 'meta read_multi_req supports optimized raw get' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'some:namspace', raw: true)
      dc.close
      dc.flush

      keys_to_query = %w[a b]

      resp = dc.get_multi(keys_to_query)

      assert_empty(resp)

      dc.set('a', 'foo')
      dc.set('b', 'bar')

      dc.set('a_really_big', 'foo')
      dc.set('big_big_really_long_key', 'bar')

      resp = dc.get_multi(keys_to_query)
      expected_resp = { 'a' => 'foo', 'b' => 'bar' }

      assert_equal(expected_resp, resp)

      resp = dc.get_multi(%w[a_really_big big_big_really_long_key])
      expected_resp = { 'a_really_big' => 'foo', 'big_big_really_long_key' => 'bar' }

      assert_equal(expected_resp, resp)
    end
  end

  it 'meta read_multi_req supports pipelined get doesnt default value on misses' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'some:namspace')
      dc.close
      dc.flush

      keys_to_query = %w[a b]

      resp = dc.get_multi(keys_to_query)

      assert_empty(resp)

      dc.set('a', 'foo')

      # Invocation without block
      resp = dc.get_multi(keys_to_query)
      expected_resp = { 'a' => 'foo' }

      assert_nil(resp['b'])
      assert_equal(expected_resp, resp)

      # Invocation with block
      dc.get_multi(keys_to_query) do |k, v|
        assert(expected_resp.key?(k) && expected_resp[k] == v)
        expected_resp.delete(k)
      end

      assert_empty expected_resp
    end
  end

  it 'raises network errors' do
    toxi_memcached_persistent(19_997, '', { down_retry_delay: 0 }) do |dc|
      dc.close
      dc.flush

      resp = dc.get_multi(%w[a b c d e f])

      assert_empty(resp)

      dc.set('a', 'foo')
      dc.set('b', 123)
      dc.set('c', %w[a b c])

      Toxiproxy[/dalli_memcached/].down do
        assert_raises Dalli::NetworkError do
          dc.get_multi(%w[a b c d e f])
        end
      end

      val = dc.get_multi(%w[a b c d e f])

      assert_equal({ 'a' => 'foo', 'b' => 123, 'c' => %w[a b c] }, val)
    end
  end

  it 'supports pipelined get with keys containing Unicode or spaces' do
    memcached_persistent do |dc|
      dc.close
      dc.flush

      keys_to_query = ['a', 'b', 'contains space', 'ƒ©åÍÎ']

      resp = dc.get_multi(keys_to_query)

      assert_empty(resp)

      dc.set('a', 'foo')
      dc.set('contains space', 123)
      dc.set('ƒ©åÍÎ', %w[a b c])

      # Invocation without block
      resp = dc.get_multi(keys_to_query)
      expected_resp = { 'a' => 'foo', 'contains space' => 123, 'ƒ©åÍÎ' => %w[a b c] }

      assert_equal(expected_resp, resp)

      # Invocation with block
      dc.get_multi(keys_to_query) do |k, v|
        assert(expected_resp.key?(k) && expected_resp[k] == v)
        expected_resp.delete(k)
      end

      assert_empty expected_resp
    end
  end

  describe 'pipeline_next_responses' do
    it 'raises NetworkError when called before pipeline_response_setup' do
      memcached_persistent do |dc|
        server = dc.send(:ring).servers.first
        server.request(:pipelined_get, %w[a b])
        assert_raises Dalli::NetworkError do
          server.pipeline_next_responses
        end
      end
    end

    it 'raises NetworkError when called after pipeline_abort' do
      memcached_persistent do |dc|
        server = dc.send(:ring).servers.first
        server.request(:pipelined_get, %w[a b])
        server.pipeline_response_setup
        server.pipeline_abort
        assert_raises Dalli::NetworkError do
          server.pipeline_next_responses
        end
      end
    end
  end
end
