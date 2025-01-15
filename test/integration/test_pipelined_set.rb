# frozen_string_literal: true

require_relative '../helper'

describe 'Pipelined Get' do
  it 'supports pipelined set' do
    memcached_persistent do |dc|
      dc.close
      dc.flush
    end
    toxi_memcached_persistent do |dc|
      dc.close
      dc.flush

      resp = dc.get_multi(%w[a b c d e f])

      assert_empty(resp)

      pairs = { 'a' => 'foo', 'b' => 123, 'c' => 'raw' }
      dc.set_multi(pairs, 60, raw: true)

      # Invocation without block
      resp = dc.get_multi(%w[a b c d e f])
      expected_resp = { 'a' => 'foo', 'b' => '123', 'c' => 'raw' }

      assert_equal(expected_resp, resp)
    end
  end

  it 'pipelined set raises network errors' do
    memcached_persistent do |dc|
      dc.close
      dc.flush
    end
    toxi_memcached_persistent(19_997, '', { down_retry_delay: 0 }) do |dc|
      dc.close
      dc.flush

      resp = dc.get_multi(%w[a b c d e f])

      assert_empty(resp)

      pairs = { 'a' => 'foo', 'b' => 123, 'c' => 'raw' }

      Toxiproxy[/dalli_memcached/].down do
        assert_raises Dalli::NetworkError do
          dc.set_multi(pairs, 60, raw: true)
        end
      end
      # Invocation without block should reconnect and not have set any keys
      resp = dc.get_multi(%w[a b c d e f])
      expected_resp = {}

      assert_equal(expected_resp, resp)
    end
  end
end
