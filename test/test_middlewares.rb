# frozen_string_literal: true

require_relative 'helper'

module Dalli
  module TestRecordingMiddleware
    def storage_req(operation, tags = {})
      (@_recorded_calls ||= []) << [:storage_req, operation, tags]
      yield
    end

    def retrieve_req(operation, tags = {})
      (@_recorded_calls ||= []) << [:retrieve_req, operation, tags]
      yield
    end

    def storage_req_pipeline(operation, tags = {})
      (@_recorded_calls ||= []) << [:storage_req_pipeline, operation, tags]
      attributes = {}
      yield attributes
    end

    def retrieve_req_pipeline(operation, tags = {})
      (@_recorded_calls ||= []) << [:retrieve_req_pipeline, operation, tags]
      attributes = {}
      yield attributes
    end

    def recorded_calls
      @_recorded_calls || []
    end
  end
end

describe 'Middlewares' do
  it 'invokes per-request middleware hooks for storage and retrieval' do
    memcached(21_451, '', { middlewares: [Dalli::TestRecordingMiddleware] }) do |dc, _|
      dc.set('mw:key', 'value', 10)
      dc.get('mw:key')

      middleware_stack = dc.send(:ring).servers.first.instance_variable_get(:@middlewares_stack)
      calls = middleware_stack.recorded_calls

      assert(calls.any? { |t, op, _| t == :storage_req && op == 'write' })
      assert(calls.any? { |t, op, _| t == :retrieve_req && op == 'read' })
    end
  end

  it 'invokes pipeline middleware for multi-ops and yields attributes' do
    memcached(21_452, '', { middlewares: [Dalli::TestRecordingMiddleware] }) do |dc, _|
      dc.set_multi({ 'a' => '1', 'b' => '2' }, 60)
      dc.get_multi('a', 'b')

      middleware_stack = dc.send(:ring).servers.first.instance_variable_get(:@middlewares_stack)
      calls = middleware_stack.recorded_calls

      assert(calls.any? { |t, op, _| t == :storage_req_pipeline && op == 'write_multi' })
      assert(calls.any? { |t, op, _| t == :retrieve_req_pipeline && op == 'read_multi' })
    end
  end
end
