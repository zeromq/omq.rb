# frozen_string_literal: true

require_relative "../../test_helper"

describe OMQ::QoS::ErrorCodes do
  it "classifies retryable vs terminal codes by bit 7" do
    assert OMQ::QoS::ErrorCodes.retryable?(OMQ::QoS::ErrorCodes::CODE_TIMEOUT)
    assert OMQ::QoS::ErrorCodes.retryable?(OMQ::QoS::ErrorCodes::CODE_INTERNAL)
    assert OMQ::QoS::ErrorCodes.retryable?(OMQ::QoS::ErrorCodes::CODE_OVERLOADED)
    refute OMQ::QoS::ErrorCodes.retryable?(OMQ::QoS::ErrorCodes::CODE_BAD_INPUT)
    refute OMQ::QoS::ErrorCodes.retryable?(OMQ::QoS::ErrorCodes::CODE_REJECTED)
  end


  it "maps QoS exceptions to their canonical codes" do
    assert_equal OMQ::QoS::ErrorCodes::CODE_TIMEOUT,
                 OMQ::QoS::ErrorCodes.exception_to_payload(OMQ::QoS::TimeoutError.new("x"))[0]
    assert_equal OMQ::QoS::ErrorCodes::CODE_BAD_INPUT,
                 OMQ::QoS::ErrorCodes.exception_to_payload(OMQ::QoS::BadInputError.new("x"))[0]
    assert_equal OMQ::QoS::ErrorCodes::CODE_OVERLOADED,
                 OMQ::QoS::ErrorCodes.exception_to_payload(OMQ::QoS::OverloadedError.new("x"))[0]
    assert_equal OMQ::QoS::ErrorCodes::CODE_REJECTED,
                 OMQ::QoS::ErrorCodes.exception_to_payload(OMQ::QoS::RejectedError.new("x"))[0]
  end


  it "maps unknown exceptions to INTERNAL (retryable)" do
    code, _ = OMQ::QoS::ErrorCodes.exception_to_payload(RuntimeError.new("boom"))
    assert_equal OMQ::QoS::ErrorCodes::CODE_INTERNAL, code
    assert OMQ::QoS::ErrorCodes.retryable?(code)
  end


  it "truncates messages longer than 65535 bytes" do
    huge = "x" * 100_000
    _, msg = OMQ::QoS::ErrorCodes.exception_to_payload(RuntimeError.new(huge))
    assert_equal OMQ::QoS::ErrorCodes::MAX_MSG_BYTES, msg.bytesize
  end


  it "round-trips through NACK wire encoding" do
    cmd = Protocol::ZMTP::Codec::Command.nack(
      "\x00" * 8,
      code:      OMQ::QoS::ErrorCodes::CODE_BAD_INPUT,
      message:   "bad frame",
      algorithm: "x",
    )
    body = cmd.instance_variable_get(:@data)
    parsed = Protocol::ZMTP::Codec::Command.new("NACK", body)
    algo, hash, code, msg = parsed.nack_data

    assert_equal "x", algo
    assert_equal "\x00".b * 8, hash
    assert_equal OMQ::QoS::ErrorCodes::CODE_BAD_INPUT, code
    assert_equal "bad frame", msg
  end
end
