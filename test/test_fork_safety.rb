# frozen_string_literal: true

require_relative 'helper'

class TestForkSafety < Minitest::Spec
  it 'remains operational after forking' do
    skip unless Process.respond_to?(:fork)

    memcached_persistent do |dc|
      dc.set('key', 'foo')

      assert_equal 'foo', dc.get('key')

      pid = fork do
        # Child process should detect fork and reconnect automatically
        100.times do |i|
          dc.set('key', "child_#{i}")
          sleep(0.01)
        end
        exit!(0)
      end

      # Parent process should continue to work
      100.times do |_i|
        begin
          dc.get('foo')
        rescue StandardError
          nil
        end
        sleep(0.01) # Add a small delay
      end

      # Wait for child to finish
      _, status = Process.wait2(pid)

      assert_predicate(status, :success?)

      # Verify we can still perform operations in parent
      dc.get('key')

      assert_kind_of String, dc.get('key'), 'Expected a string value from memcached'
    end
  end
end
