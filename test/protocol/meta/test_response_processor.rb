# frozen_string_literal: true

require_relative '../../helper'
require 'stringio'

class ResponseProcessorTestIO
  attr_reader :read_calls

  def initialize(*chunks)
    @io = StringIO.new(chunks.join)
    @read_calls = []
  end

  def read_line
    @io.gets("\r\n")
  end

  def read(length)
    @read_calls << length
    @io.read(length)
  end
end

class PipelinedResponseErrorTestBuffer
  attr_reader :processed_count

  def initialize(*responses)
    @responses = responses
    @processed_count = 0
    @cleared = false
  end

  def read; end

  def process_single_getk_response
    @processed_count += 1
    @responses.shift || [nil, nil, nil, nil, nil]
  end

  def clear
    @cleared = true
  end

  def in_progress?
    !@cleared
  end
end

class PipelinedResponseErrorTestConnectionManager
  attr_reader :finished

  def finish_request!
    @finished = true
  end

  def error_on_request!(err)
    raise err
  end
end

describe Dalli::Protocol::Meta::ResponseProcessor do
  let(:correlation_failures) { [] }

  def processor_for(io, marshaller = nil)
    Dalli::Protocol::Meta::ResponseProcessor.new(
      io, marshaller, on_correlation_failure: ->(*details) { correlation_failures << details }
    )
  end

  response_codes = %w[VA EN HD]
  [' Owrong-token', ' O', ''].each do |opaque_flag|
    response_codes.each do |code|
      it "checks #{code} responses with opaque #{opaque_flag.inspect} before reading a body" do
        response_cases = [
          [:meta_get_with_value, {}, nil],
          [:meta_get_with_value, { skip_flags: true }, nil],
          [:meta_get_with_value, { cache_nils: true }, Dalli::NOT_FOUND],
          [:meta_get_with_value_and_cas, {}, [nil, 0]],
          [:meta_get_with_value_and_meta_flags, {}, [nil, {}]],
          [:meta_get_with_value_and_meta_flags, { cache_nils: true }, [Dalli::NOT_FOUND, {}]],
          [:meta_get_with_status, {}, nil]
        ]
        response_cases << [:meta_get_without_value, {}, nil] unless code == 'VA'
        response = code == 'VA' ? "VA 4 f1#{opaque_flag}\r\nNOPE\r\n" : "#{code}#{opaque_flag}\r\n"

        response_cases.each do |method_name, options, expected|
          correlation_failures.clear
          io = ResponseProcessorTestIO.new(response)
          marshaller = Dalli::Protocol::ValueMarshaller.new({})
          processor = processor_for(io, marshaller)

          result = processor.public_send(method_name, **options, expected_opaque: 'expected-token')
          if method_name == :meta_get_with_status
            assert_predicate result.first, :miss?
            assert_equal 0, result.last
          elsif expected.nil?
            assert_nil result
          else
            assert_equal expected, result
          end

          received = opaque_flag.empty? ? nil : opaque_flag.strip[1..]

          assert_equal [['expected-token', received, code]], correlation_failures
          assert_empty io.read_calls, 'an uncorrelated body must not be read or deserialized'
        end
      end
    end
  end

  it 'extracts opaque strings and returns nil only when the flag is absent' do
    processor = processor_for(nil)

    assert_nil processor.opaque_from_tokens(%w[EN])
    assert_equal '', processor.opaque_from_tokens(%w[EN O])
    assert_equal 'token', processor.opaque_from_tokens(%w[VA 5 f0 Otoken])
  end

  it 'preserves processor behavior when no expected opaque is supplied' do
    io = ResponseProcessorTestIO.new("HD\r\n")
    processor = processor_for(io)

    assert processor.meta_get_without_value
    assert_empty correlation_failures
  end

  it 'accepts a get response containing the expected opaque token' do
    marshaller = Object.new
    def marshaller.retrieve(value, _flags)
      value
    end
    io = ResponseProcessorTestIO.new("VA 5 f0 Oexpected-token\r\n", 'value', "\r\n")
    processor = processor_for(io, marshaller)

    assert_equal 'value', processor.meta_get_with_value(expected_opaque: 'expected-token')
    assert_empty correlation_failures
    assert_equal [5, 2], io.read_calls
  end

  %w[EN HD].each do |code|
    it "keeps the connection for #{code} with the expected opaque" do
      io = ResponseProcessorTestIO.new("#{code} Oexpected-token\r\n")
      processor = processor_for(io)

      result = processor.meta_get_without_value(expected_opaque: 'expected-token')

      code == 'EN' ? assert_nil(result) : assert(result)

      assert_empty correlation_failures
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
      processor = processor_for(io)

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
      processor = processor_for(io)

      err = assert_raises(Dalli::ServerError) do
        processor.meta_set_with_cas
      end

      assert_equal line.chomp("\r\n"), err.message
    end
  end

  it 'records pipelined error responses instead of treating them as values or terminators' do
    response_cases = [
      ["CLIENT_ERROR invalid flag\r\n", Dalli::DalliError, 'Response error: CLIENT_ERROR invalid flag'],
      [
        "SERVER_ERROR proxy write to backend failed\r\n",
        Dalli::ServerError,
        'SERVER_ERROR proxy write to backend failed'
      ],
      ["ERROR\r\n", Dalli::DalliError, 'Response error: ERROR']
    ]

    processor = Dalli::Protocol::Meta::ResponseProcessor.new(nil, nil)

    response_cases.each do |line, error_class, error_message|
      bytes, status, cas, key, value, error = processor.getk_response_from_buffer(line)

      assert_equal line.bytesize, bytes
      refute(status)
      assert_nil cas
      assert_nil key
      assert_nil value
      assert_instance_of error_class, error
      assert_equal error_message, error.message
    end
  end

  it 'identifies MN as the pipelined noop terminator' do
    processor = Dalli::Protocol::Meta::ResponseProcessor.new(nil, nil)

    bytes, status, cas, key, value, error = processor.getk_response_from_buffer("MN\r\n")

    assert_equal "MN\r\n".bytesize, bytes
    assert(status)
    assert_nil cas
    assert_nil key
    assert_nil value
    assert_nil error
  end

  it 'raises and drains the pipeline when the first pipelined response is CLIENT_ERROR' do
    assert_pipelined_error_drained(
      [false, nil, nil, nil, Dalli::DalliError.new('Response error: CLIENT_ERROR invalid flag')],
      valid_pipeline_value('a', 'foo'),
      pipeline_terminator,
      error_class: Dalli::DalliError,
      error_message: 'Response error: CLIENT_ERROR invalid flag'
    )
  end

  it 'raises and drains the pipeline when a middle pipelined response is SERVER_ERROR' do
    assert_pipelined_error_drained(
      valid_pipeline_value('a', 'foo'),
      [false, nil, nil, nil, Dalli::ServerError.new('SERVER_ERROR proxy write to backend failed')],
      valid_pipeline_value('b', 'bar'),
      pipeline_terminator,
      error_class: Dalli::ServerError,
      error_message: 'SERVER_ERROR proxy write to backend failed'
    )
  end

  it 'raises and drains the pipeline when the last pipelined response is ERROR' do
    assert_pipelined_error_drained(
      valid_pipeline_value('a', 'foo'),
      valid_pipeline_value('b', 'bar'),
      [false, nil, nil, nil, Dalli::DalliError.new('Response error: ERROR')],
      pipeline_terminator,
      error_class: Dalli::DalliError,
      error_message: 'Response error: ERROR'
    )
  end

  def valid_pipeline_value(key, value)
    [true, nil, key, value, nil]
  end

  def pipeline_terminator
    [true, nil, nil, nil, nil]
  end

  def assert_pipelined_error_drained(*responses, error_class:, error_message:)
    buffer = PipelinedResponseErrorTestBuffer.new(*responses)
    connection_manager = PipelinedResponseErrorTestConnectionManager.new
    server = Dalli::Protocol::Base.allocate
    server.instance_variable_set(:@response_buffer, buffer)
    server.instance_variable_set(:@connection_manager, connection_manager)

    err = assert_raises(error_class) do
      server.pipeline_next_responses
    end

    assert_equal error_message, err.message
    assert_equal responses.length, buffer.processed_count
    assert connection_manager.finished
    refute_predicate(buffer, :in_progress?)
  end
end
