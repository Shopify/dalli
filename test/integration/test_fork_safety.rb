# frozen_string_literal: true

require_relative '../helper'

describe 'Fork safety' do
  next unless Process.respond_to?(:fork)

  it 'automatically reconnects after fork' do
    memcached_persistent do |dc|
      dc.set('fork_test_key', 'parent_value')

      assert_equal 'parent_value', dc.get('fork_test_key')

      manager = dc.send(:ring).servers.first.instance_variable_get(:@connection_manager)
      parent_random = manager.instance_variable_get(:@opaque_random)
      expected_parent_token = parent_random.dup.urlsafe_base64(8, false)

      # Fork a child process
      read_pipe, write_pipe = IO.pipe
      pid = fork do
        read_pipe.close

        # In the child process, we should detect the fork and reconnect
        begin
          dc.set('child_key', 'child_value')
          value = dc.get('child_key')

          child_random = manager.instance_variable_get(:@opaque_random)
          write_pipe.write("success:#{value}\n#{child_random.equal?(parent_random)}\n#{child_random.seed}")
        rescue StandardError => e
          write_pipe.write("error:#{e.class.name}:#{e.message}")
        ensure
          write_pipe.close
          exit!(0)
        end
      end

      # In the parent process
      write_pipe.close

      # Wait for child process to finish
      Process.wait(pid)

      # Read result from pipe
      result, inherited_generator, child_seed = read_pipe.read.split("\n")
      read_pipe.close

      # Verify the child successfully reconnected and performed operations
      assert_match(/^success:/, result, "Child process encountered an error: #{result}")
      assert_equal 'success:child_value', result
      assert_equal 'false', inherited_generator
      refute_equal parent_random.seed.to_s, child_seed
      assert_same parent_random, manager.instance_variable_get(:@opaque_random)
      assert_equal expected_parent_token, manager.generate_opaque

      # Parent should still be able to work
      assert_equal 'parent_value', dc.get('fork_test_key')
    end
  end
end
