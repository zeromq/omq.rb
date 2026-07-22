# frozen_string_literal: true

require_relative "test_helper"

describe Protocol::ZMTP::Z85 do
  Z85 = Protocol::ZMTP::Z85

  it "roundtrips binary data" do
    data = SecureRandom.random_bytes(32)
    encoded = Z85.encode(data)
    decoded = Z85.decode(encoded)
    assert_equal data, decoded
  end

  it "encodes the RFC 32 test vector" do
    data = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B].pack("C*")
    encoded = Z85.encode(data)
    decoded = Z85.decode(encoded)
    assert_equal data, decoded
    assert_equal 10, encoded.bytesize  # 8 bytes → 10 Z85 chars
  end

  it "rejects data not a multiple of 4" do
    assert_raises(ArgumentError) { Z85.encode("abc") }
  end

  it "rejects string not a multiple of 5" do
    assert_raises(ArgumentError) { Z85.decode("abcd") }
  end
end
