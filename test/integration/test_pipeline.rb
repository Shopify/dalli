# frozen_string_literal: true

require_relative '../helper'

describe 'pipelined' do
  MemcachedManager.supported_protocols.each do |p|
    describe "using the #{p} protocol" do
      describe 'get' do
        it 'returns the value on a hit' do
          memcached_persistent(p) do |dc|
            dc.flush

            val1 = '1234567890' * 999_999
            dc.set('a', val1)

            results = dc.pipelined do |pipeline|
              pipeline.get('a')
            end

            assert_equal [val1], results
          end
        end
      end
    end
  end
end
