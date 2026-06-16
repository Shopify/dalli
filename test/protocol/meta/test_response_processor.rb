# frozen_string_literal: true

require_relative '../../helper'

class ResponseProcessorTestIO
  def initialize(*lines)
    @lines = lines
  end

  def read_line
    @lines.shift&.dup
  end
end

describe Dalli::Protocol::Meta::ResponseProcessor do
  it 'includes the full unexpected response line in DalliError messages' do
    response_cases = [
      [
        # e.g. an unsupported/unknown meta-protocol flag reaches memcached.
        :meta_set_with_cas,
        "CLIENT_ERROR invalid flag\r\n",
        'Response error: CLIENT_ERROR invalid flag'
      ],
      [
        # e.g. a malformed numeric token such as an invalid TTL reaches memcached.
        :meta_delete,
        "CLIENT_ERROR bad token in command line format\r\n",
        'Response error: CLIENT_ERROR bad token in command line format'
      ],
      [
        # e.g. a request uses the base64-key flag but the key is not decodable.
        :meta_get_with_value,
        "CLIENT_ERROR error decoding key\r\n",
        'Response error: CLIENT_ERROR error decoding key'
      ]
    ]

    response_cases.each do |method_name, line, expected_message|
      io = ResponseProcessorTestIO.new(line)
      processor = Dalli::Protocol::Meta::ResponseProcessor.new(io, nil)

      err = assert_raises(Dalli::DalliError) do
        processor.public_send(method_name)
      end

      assert_instance_of Dalli::DalliError, err
      assert_equal expected_message, err.message
    end
  end

  it 'includes the full server error response line in ServerError messages' do
    response_cases = [
      # e.g. the serialized/compressed value exceeds memcached's item size limit.
      "SERVER_ERROR object too large for cache\r\n",
      # e.g. memcached cannot allocate memory for the item being stored.
      "SERVER_ERROR out of memory storing object\r\n",
      # e.g. a proxy/router reports a backend write failure as a server error.
      "SERVER_ERROR proxy write to backend failed\r\n"
    ]

    response_cases.each do |line|
      io = ResponseProcessorTestIO.new(line)
      processor = Dalli::Protocol::Meta::ResponseProcessor.new(io, nil)

      err = assert_raises(Dalli::ServerError) do
        processor.meta_set_with_cas
      end

      assert_equal line.chomp("\r\n"), err.message
    end
  end
end
