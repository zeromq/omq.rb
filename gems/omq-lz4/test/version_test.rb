# frozen_string_literal: true

require_relative "test_helper"

describe OMQ::LZ4 do
  it "defines a non-empty VERSION string" do
    assert_instance_of String, OMQ::LZ4::VERSION
    refute_empty OMQ::LZ4::VERSION
  end

  it "defines ProtocolError as a StandardError subclass" do
    assert_includes OMQ::LZ4::ProtocolError.ancestors, StandardError
  end
end
