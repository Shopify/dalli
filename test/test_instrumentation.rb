# frozen_string_literal: true

require_relative 'helper'
require 'dalli/instrumentation'

describe 'Instrumentation' do
  def setup
    memcached_persistent do |dc, _port|
      @client = dc
    end
    @client.set(SecureRandom.uuid, SecureRandom.uuid) # prevent capturing test setup spans
    OTEL_EXPORTER.reset
  end

  def teardown
    @client&.close
    memcached_kill(21_345)
  end

  describe 'get operation' do
    it 'instruments get with correct span name and tags' do
      @client.set('test_key', 'test_value')
      OTEL_EXPORTER.reset

      assert_equal 'test_value', @client.get('test_key')

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.get', span.name
      assert_equal :client, span.kind
      assert_equal 'memcached', span.attributes['db.system']
      assert_equal 'test_key', span.attributes['keys']
    end
  end

  describe 'set operation' do
    it 'instruments set with correct span name and tags' do
      @client.set('test_key', 'test_value', 300)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.set', span.name
      assert_equal :client, span.kind
      assert_equal 'memcached', span.attributes['db.system']
      assert_equal 'test_key', span.attributes['keys']
      assert_equal 300, span.attributes['ttl']
    end
  end

  describe 'add operation' do
    it 'instruments add with correct span name and tags' do
      @client.delete('new_key')
      OTEL_EXPORTER.reset

      @client.add('new_key', 'new_value', 600)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.add', span.name
      assert_equal 'new_key', span.attributes['keys']
      assert_equal 600, span.attributes['ttl']
    end
  end

  describe 'replace operation' do
    it 'instruments replace with correct span name and tags' do
      @client.set('existing_key', 'old_value')
      OTEL_EXPORTER.reset

      @client.replace('existing_key', 'new_value', 400)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.replace', span.name
      assert_equal 'existing_key', span.attributes['keys']
      assert_equal 400, span.attributes['ttl']
    end
  end

  describe 'delete operation' do
    it 'instruments delete with correct span name and tags' do
      @client.set('delete_key', 'value')
      OTEL_EXPORTER.reset

      @client.delete('delete_key')

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.delete', span.name
      assert_equal 'delete_key', span.attributes['keys']
      assert_equal 0, span.attributes['cas']
    end
  end

  describe 'append operation' do
    it 'instruments append with correct span name and tags' do
      @client.set('append_key', 'hello', nil, raw: true)
      OTEL_EXPORTER.reset

      @client.append('append_key', ' world')

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.append', span.name
      assert_equal 'append_key', span.attributes['keys']
    end
  end

  describe 'prepend operation' do
    it 'instruments prepend with correct span name and tags' do
      @client.set('prepend_key', 'world', nil, raw: true)
      OTEL_EXPORTER.reset

      @client.prepend('prepend_key', 'hello ')

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.prepend', span.name
      assert_equal 'prepend_key', span.attributes['keys']
    end
  end

  describe 'incr operation' do
    it 'instruments incr with correct span name and tags' do
      @client.set('counter', '10', 0, raw: true)
      OTEL_EXPORTER.reset

      assert_equal 15, @client.incr('counter', 5, 3600, 0)
      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size
      span = spans.first

      assert_equal 'memcached.decr_incr', span.name
      assert_equal 'counter', span.attributes['keys']
      assert_equal 5, span.attributes['delta']
      assert_equal 3600, span.attributes['ttl']
      assert_equal 0, span.attributes['initial']
      assert span.attributes['incr']
    end
  end

  describe 'decr operation' do
    it 'instruments decr with correct span name and tags' do
      @client.set('counter', '20', 0, raw: true)
      OTEL_EXPORTER.reset

      assert_equal 17, @client.decr('counter', 3, 1800, 0)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.decr_incr', span.name
      assert_equal 'counter', span.attributes['keys']
      assert_equal 3, span.attributes['delta']
      assert_equal 1800, span.attributes['ttl']
      assert_equal 0, span.attributes['initial'] # nil defaults to 0
      refute span.attributes['incr']
    end
  end

  describe 'flush operation' do
    it 'instruments flush with correct span name and tags' do
      OTEL_EXPORTER.reset

      @client.flush(10)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size # number of servers in the ring in test

      span = spans.first

      assert_equal 'memcached.flush', span.name
      assert_equal 10, span.attributes['delay']
    end
  end

  describe 'version operation' do
    it 'instruments version with correct span name' do
      OTEL_EXPORTER.reset

      @client.version

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size # number of servers in the ring in test

      span = spans.first

      assert_equal 'memcached.version', span.name
    end
  end

  describe 'get_multi operation' do
    it 'instruments read_multi with correct span name and tags' do
      @client.set('key1', 'value1')
      @client.set('key2', 'value2')
      @client.set('key3', 'value3')
      OTEL_EXPORTER.reset

      result = @client.get_multi('key1', 'key2', 'key3')

      assert_equal({ 'key1' => 'value1', 'key2' => 'value2', 'key3' => 'value3' }, result)
      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.read_multi', span.name
      assert_equal %w[key1 key2 key3], span.attributes['keys']
    end
  end

  describe 'set_multi operation' do
    it 'instruments write_multi with correct span name and tags' do
      pairs = { 'multi1' => 'value1', 'multi2' => 'value2', 'multi3' => 'value3' }
      OTEL_EXPORTER.reset

      @client.set_multi(pairs, 7200)

      spans = OTEL_EXPORTER.finished_spans

      assert_equal 1, spans.size

      span = spans.first

      assert_equal 'memcached.write_multi', span.name
      assert_equal %w[multi1 multi2 multi3], span.attributes['keys']
      assert_equal 7200, span.attributes['ttl']
    end
  end
end
