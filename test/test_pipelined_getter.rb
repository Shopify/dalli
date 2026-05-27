# frozen_string_literal: true

require_relative 'helper'

class FakePipelinedServer
  attr_reader :name, :aborted

  def initialize(name:, responses: {}, error: nil)
    @name = name
    @responses = responses
    @error = error
    @aborted = false
  end

  def connected?
    true
  end

  def pipeline_next_responses
    raise @error if @error

    @responses
  end

  def pipeline_complete?
    true
  end

  def pipeline_abort
    @aborted = true
  end
end

describe Dalli::PipelinedGetter do
  let(:ring) { Struct.new(:servers).new([]) }
  let(:key_manager) { Dalli::KeyManager.new({}) }
  let(:getter) { Dalli::PipelinedGetter.new(ring, key_manager) }

  it 'treats a server read error as misses and continues processing the batch' do
    bad_server = FakePipelinedServer.new(name: 'bad:11211', error: Dalli::NetworkError.new('boom'))
    good_server = FakePipelinedServer.new(name: 'good:11211', responses: { 'hit' => ['value', nil] })
    results = {}

    getter.stub(:servers_with_response, [bad_server, good_server]) do
      remaining_servers = getter.fetch_responses([bad_server, good_server], Time.now, 1) do |key, value_list|
        results[key] = value_list.first
      end

      assert_empty remaining_servers
    end

    assert_equal({ 'hit' => 'value' }, results)
    assert_predicate bad_server, :aborted
  end
end
