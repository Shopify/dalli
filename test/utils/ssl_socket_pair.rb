# frozen_string_literal: true

require 'openssl'
require 'socket'
require_relative 'certificate_generator'

##
# A connected TLS client/server pair over loopback TCP, using the test certificates. The client is a
# Dalli::Socket::SSLSocket, as Dalli would create.
##
module SSLSocketPair
  def self.open
    listener = TCPServer.new('127.0.0.1', 0)
    client_tcp = TCPSocket.new('127.0.0.1', listener.local_address.ip_port)
    server = OpenSSL::SSL::SSLSocket.new(listener.accept, server_context)
    server.sync_close = true
    accepting = Thread.new { server.accept }
    client = Dalli::Socket::SSLSocket.new(client_tcp, CertificateGenerator.ssl_context)
    client.hostname = 'localhost'
    client.sync_close = true
    client.connect
    accepting.join
    [client, server]
  ensure
    listener&.close
  end

  def self.server_context
    context = OpenSSL::SSL::SSLContext.new
    context.cert = OpenSSL::X509::Certificate.new(File.read(CertificateGenerator::MEMCACHED_CERT_PATH))
    context.key = OpenSSL::PKey.read(File.read(CertificateGenerator::MEMCACHED_PK_PATH))
    context
  end
end
