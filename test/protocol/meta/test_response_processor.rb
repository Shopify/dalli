# frozen_string_literal: true

require_relative '../../helper'

class ResponseProcessorTestIO
  def initialize(*lines)
    @lines = lines
  end

  def read_line
    @lines.shift&.dup
  end

  def read(_size)
    @lines.shift&.dup
  end
end

class ResponseProcessorTestValueMarshaller
  def retrieve(value, _bitflags)
    value
  end
end

describe Dalli::Protocol::Meta::ResponseProcessor do
  describe '#meta_get_with_value_and_cas' do
    it 'returns a stale hit CacheResult with the value and CAS token' do
      io = ResponseProcessorTestIO.new("VA 5 f0 c42 X\r\n", 'value', "\r\n")
      marshaller = ResponseProcessorTestValueMarshaller.new
      processor = Dalli::Protocol::Meta::ResponseProcessor.new(io, marshaller)

      result = processor.meta_get_with_value_and_cas

      assert_instance_of Dalli::CacheResult, result
      assert_equal 'value', result.value
      assert_equal 42, result.cas_token
      assert_predicate result, :hit?
      refute_predicate result, :miss?
      assert_predicate result, :stale?
    end

    it 'returns a miss CacheResult with CAS token zero for an EN response' do
      io = ResponseProcessorTestIO.new("EN\r\n")
      processor = Dalli::Protocol::Meta::ResponseProcessor.new(io, nil)

      result = processor.meta_get_with_value_and_cas

      assert_instance_of Dalli::CacheResult, result
      assert_nil result.value
      assert_equal 0, result.cas_token
      assert_predicate result, :miss?
      refute_predicate result, :hit?
      refute_predicate result, :stale?
    end

    it 'returns a miss CacheResult with the response CAS token for an HD response' do
      io = ResponseProcessorTestIO.new("HD c42\r\n")
      processor = Dalli::Protocol::Meta::ResponseProcessor.new(io, nil)

      result = processor.meta_get_with_value_and_cas

      assert_nil result.value
      assert_equal 42, result.cas_token
      assert_predicate result, :miss?
      refute_predicate result, :hit?
    end
  end

  it 'includes the full unexpected response line in DalliError messages' do
    # Representative CLIENT_ERROR lines returned by memcached. The response
    # processor treats each as an unexpected response code, but should preserve
    # the full response line in the raised DalliError.

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
