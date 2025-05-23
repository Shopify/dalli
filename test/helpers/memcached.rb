# frozen_string_literal: true

require 'socket'
require_relative '../utils/certificate_generator'
require_relative '../utils/memcached_manager'

module Memcached
  module Helper
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
    def memcached(port_or_socket, args = '', client_options = {}, terminate_process: true)
      dc = MemcachedManager.start_and_flush_with_retry(port_or_socket, args, client_options)
      yield dc, port_or_socket if block_given?
      memcached_kill(port_or_socket) if terminate_process
    end

    # Launches a memcached process using the memcached method in this module,
    # but sets terminate_process to false ensuring that the process persists
    # past execution of the block argument.
    def memcached_persistent(port_or_socket = 21_345, args = '', client_options = {}, &)
      memcached(port_or_socket, args, client_options, terminate_process: false, &)
    end

    ###
    # Launches a persistent memcached process that is proxied through Toxiproxy
    # to test network errors.
    # uses port 21347 for the Toxiproxy proxy port and the specified port_or_socket
    # for the memcached process.
    ###
    def toxi_memcached_persistent(
      port = MemcachedManager::TOXIPROXY_UPSTREAM_PORT,
      args = '',
      client_options = {},
      &
    )
      raise 'Toxiproxy does not support unix sockets' if port.to_i.zero?

      unless @toxy_configured
        Toxiproxy.populate([{
                             name: 'dalli_memcached',
                             listen: "localhost:#{MemcachedManager::TOXIPROXY_MEMCACHED_PORT}",
                             upstream: "localhost:#{port}"
                           }])
      end
      @toxy_configured ||= true
      memcached_persistent(port, args, client_options) do |dc, _|
        dc.close # We don't need the client to talk directly to memcached
      end
      dc = Dalli::Client.new("localhost:#{MemcachedManager::TOXIPROXY_MEMCACHED_PORT}", client_options)
      yield dc, port
    end

    # Launches a persistent memcached process, configured to use SSL
    def memcached_ssl_persistent(port_or_socket = rand(21_397..21_896), &)
      memcached_persistent(port_or_socket,
                           CertificateGenerator.ssl_args,
                           { ssl_context: CertificateGenerator.ssl_context },
                           &)
    end

    # Kills the memcached process that was launched using this helper on hte
    # specified port_or_socket.
    def memcached_kill(port_or_socket)
      MemcachedManager.stop(port_or_socket)
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
