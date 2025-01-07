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

      $DEBUG = false
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
    ensure
      $DEBUG = false
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

  it 'handles network errors' do
    toxi_memcached_persistent do |dc|
      dc.close
      dc.flush

      resp = dc.get_multi(%w[a b c d e f])

      assert_empty(resp)

      dc.set('a', 'foo')
      dc.set('b', 123)
      dc.set('c', %w[a b c])

      Toxiproxy[/dalli_memcached/].down do
        resp = dc.get_multi(%w[a b c d e f])
        expected_resp = {}

        assert_equal(expected_resp, resp)
      end
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

  describe 'pipelined_get_responses' do
    it 'raises standard error when called before requests are made' do
      memcached_persistent do |dc|
        server = dc.send(:ring).servers.first
        assert_raises(StandardError, '[Dalli] No request in progress. This may be a bug in Dalli.') do
          server.pipelined_get_responses
        end
      end
    end

    it 'raises standard error when called after pipeline_abort' do
      memcached_persistent do |dc|
        server = dc.send(:ring).servers.first
        server.request(:pipelined_get_request, %w[a b])
        server.pipeline_abort
        assert_raises(StandardError, '[Dalli] No request in progress. This may be a bug in Dalli.') do
          server.pipelined_get_responses
        end
      end
    end
  end
end
