# frozen_string_literal: true

require 'socket'
require 'tempfile'
require_relative '../utils/certificate_generator'
require_relative '../utils/memcached_manager'

module Memcached
  module Helper
    UNIX_SOCKET_PATH = (f = Tempfile.new('dalli_test')
                        f.close
                        f.path)
    # Launches a memcached process with the specified arguments.  Takes
    # a block to which an initialized Dalli::Client and the port_or_socket
    # is passed.
    #
    # port_or_socket - If numeric or numeric string, treated as a TCP port
    #                  on localhost.  If not, treated as a UNIX domain socket
    # args - Command line args passed to the memcached invocation
    # client_options - Options passed to the Dalli::Client on initialization
    # terminate_process - whether to terminate the memcached process on
    #                     exiting the block
    def memcached(protocol, port_or_socket, args = '', client_options = {}, terminate_process: true)
      dc = MemcachedManager.start_and_flush_with_retry(port_or_socket, args, client_options.merge(protocol: protocol))
      yield dc, port_or_socket if block_given?
      memcached_kill(port_or_socket) if terminate_process
    end

    # Launches a memcached process using the memcached method in this module,
    # but sets terminate_process to false ensuring that the process persists
    # past execution of the block argument.
    # rubocop:disable Metrics/ParameterLists
    def memcached_persistent(protocol = :binary, port_or_socket = 21_345, args = '', client_options = {}, &block)
      memcached(protocol, port_or_socket, args, client_options, terminate_process: false, &block)
    end
    # rubocop:enable Metrics/ParameterLists

    ###
    # Launches a persistent memcached process that is proxied through Toxiproxy
    # to test network errors.
    # uses port 21347 for the Toxiproxy proxy port and the specified port_or_socket
    # for the memcached process.
    ###
    def toxi_memcached_persistent(protocol = :binary, args = '', client_options = {}, &block)
      unless @toxy_configured
        Toxiproxy.populate([{
                             name: 'dalli_memcached',
                             listen: "localhost:#{MemcachedManager::TOXIPROXY_MEMCACHED_PORT}",
                             upstream: 'localhost:21345'
                           }])
      end
      @toxy_configured ||= true
      memcached_persistent(protocol, MemcachedManager::TOXIPROXY_MEMCACHED_PORT, args, client_options, &block)
    end

    # Launches a persistent memcached process, configured to use SSL
    def memcached_ssl_persistent(protocol = :binary, port_or_socket = rand(21_397..21_896), &block)
      memcached_persistent(protocol,
                           port_or_socket,
                           CertificateGenerator.ssl_args,
                           { ssl_context: CertificateGenerator.ssl_context },
                           &block)
    end

    # Kills the memcached process that was launched using this helper on hte
    # specified port_or_socket.
    def memcached_kill(port_or_socket)
      MemcachedManager.stop(port_or_socket)
    end

    # Launches a persistent memcached process, configured to use SASL authentication
    def memcached_sasl_persistent(port_or_socket = 21_398, &block)
      memcached_persistent(:binary, port_or_socket, '-S', sasl_credentials, &block)
    end

    # The SASL credentials used for the test SASL server
    def sasl_credentials
      { username: 'testuser', password: 'testtest' }
    end

    private

    def kill_process(pid)
      return unless pid

      Process.kill('TERM', pid)
      Process.wait(pid)
    end

    def supports_fork?
      Process.respond_to?(:fork)
    end
  end
end
