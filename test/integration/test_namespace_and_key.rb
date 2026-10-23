# frozen_string_literal: true

require_relative '../helper'

describe 'Namespace and key behavior' do
  it 'handles namespaced keys' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'a')
      dc.set('namespaced', 1)
      dc2 = Dalli::Client.new("localhost:#{port}", namespace: 'b')
      dc2.set('namespaced', 2)

      assert_equal 1, dc.get('namespaced')
      assert_equal 2, dc2.get('namespaced')
    end
  end

  it 'handles a nil namespace' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: nil)
      dc.set('key', 1)

      assert_equal 1, dc.get('key')
    end
  end

  it 'truncates cache keys that are too long' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'some:namspace')
      key = 'this-cache-key-is-far-too-long-so-it-must-be-hashed-and-truncated-and-stuff' * 10
      value = 'some value'

      assert op_addset_succeeds(dc.set(key, value))
      assert_equal value, dc.get(key)
    end
  end

  it 'handles namespaced keys in get_multi' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: 'a')
      dc.set('a', 1)
      dc.set('b', 2)

      assert_equal({ 'a' => 1, 'b' => 2 }, dc.get_multi('a', 'b'))
    end
  end

  it 'handles special Regexp characters in namespace with get_multi' do
    memcached_persistent do |_, port|
      # /(?!)/ is a contradictory PCRE and should never be able to match
      dc = Dalli::Client.new("localhost:#{port}", namespace: '(?!)')
      dc.set('a', 1)
      dc.set('b', 2)

      assert_equal({ 'a' => 1, 'b' => 2 }, dc.get_multi('a', 'b'))
    end
  end

  it 'allows whitespace characters in keys' do
    memcached_persistent do |dc|
      dc.set "\t", 1

      assert_equal 1, dc.get("\t")
      dc.set "\n", 1

      assert_equal 1, dc.get("\n")
      dc.set '   ', 1

      assert_equal 1, dc.get('   ')
    end
  end

  it 'does not allow blanks for keys' do
    memcached_persistent do |dc|
      assert_raises ArgumentError do
        dc.set '', 1
      end
      assert_raises ArgumentError do
        dc.set nil, 1
      end
    end
  end

  it 'allow the namespace to be a symbol' do
    memcached_persistent do |_, port|
      dc = Dalli::Client.new("localhost:#{port}", namespace: :wunderschoen)
      dc.set 'x' * 251, 1

      assert_equal(1, dc.get(('x' * 251).to_s))
    end
  end

  # Single-server rings take the pipelined set_multi path; larger rings fall back to quiet sets.
  def with_ring(server_count, namespace)
    memcached_persistent do |_, first_port|
      memcached_persistent(21_355) do |_, second_port|
        servers = ["localhost:#{first_port}", "localhost:#{second_port}"].first(server_count)
        dc = Dalli::Client.new(servers, namespace: namespace, raw: true)
        dc.flush
        yield dc
      ensure
        dc&.close
      end
    end
  end

  [1, 2].product([nil, 'some:namspace']).each do |server_count, namespace|
    it "set_multi keys match get and get_multi (#{server_count} servers, namespace #{namespace.inspect})" do
      with_ring(server_count, namespace) do |dc|
        pairs = { 'plain' => 'a', 'has space' => 'b', "tab\tkey" => 'c', 'naïve' => 'd', 'ключ' => 'e' }
        dc.set_multi(pairs, 60)

        assert_equal pairs, dc.get_multi(pairs.keys)
        pairs.each { |key, value| assert_equal value, dc.get(key), key }
      end
    end
  end

  [1, 2].each do |server_count|
    it "set_multi accepts the non-String keys set accepts (#{server_count} servers)" do
      with_ring(server_count, nil) do |dc|
        dc.set_multi({ sym: 'a', 42 => 'b' }, 60)

        assert_equal 'a', dc.get(:sym)
        assert_equal 'b', dc.get(42)
      end
    end
  end
end
