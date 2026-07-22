# frozen_string_literal: true

require_relative "../test_helper"

describe Protocol::ZMTP::Codec::Greeting do
  Greeting = Protocol::ZMTP::Codec::Greeting

  describe ".encode" do
    it "produces a 64-byte binary string" do
      data = Greeting.encode
      assert_equal 64, data.bytesize
      assert_equal Encoding::BINARY, data.encoding
    end

    it "starts with signature 0xFF...0x7F" do
      data = Greeting.encode
      assert_equal 0xFF, data.getbyte(0)
      assert_equal 0x7F, data.getbyte(9)
    end

    it "has zero padding in bytes 1-8" do
      data = Greeting.encode
      (1..8).each { |i| assert_equal 0x00, data.getbyte(i), "byte #{i} should be 0x00" }
    end

    it "encodes version 3.1" do
      data = Greeting.encode
      assert_equal 3, data.getbyte(10)
      assert_equal 1, data.getbyte(11)
    end

    it "encodes NULL mechanism null-padded" do
      data = Greeting.encode(mechanism: "NULL")
      mechanism = data.byteslice(12, 20)
      assert_equal "NULL", mechanism.delete("\x00")
      assert_equal 20, mechanism.bytesize
    end

    it "encodes as_server flag" do
      client = Greeting.encode(as_server: false)
      server = Greeting.encode(as_server: true)
      assert_equal 0, client.getbyte(32)
      assert_equal 1, server.getbyte(32)
    end

    it "has zero filler in bytes 33-63" do
      data = Greeting.encode
      (33..63).each { |i| assert_equal 0x00, data.getbyte(i), "byte #{i} should be 0x00" }
    end
  end

  describe ".decode" do
    it "round-trips NULL mechanism" do
      result = Greeting.decode(Greeting.encode(mechanism: "NULL", as_server: true))
      assert_equal 3, result[:major]
      assert_equal 1, result[:minor]
      assert_equal "NULL", result[:mechanism]
      assert result[:as_server]
    end

    it "round-trips CURVE mechanism" do
      result = Greeting.decode(Greeting.encode(mechanism: "CURVE", as_server: true))
      assert_equal "CURVE", result[:mechanism]
      assert result[:as_server]
    end

    it "decodes as_server false" do
      result = Greeting.decode(Greeting.encode(as_server: false))
      refute result[:as_server]
    end

    it "accepts ZMTP version 3.0" do
      v30 = Greeting.encode.dup
      v30.setbyte(11, 0)
      result = Greeting.decode(v30)
      assert_equal 3, result[:major]
      assert_equal 0, result[:minor]
    end

    it "raises on short data" do
      assert_raises(Protocol::ZMTP::Error) { Greeting.decode("short") }
    end

    it "raises on bad signature" do
      bad = Greeting.encode.dup
      bad.setbyte(0, 0x00)
      assert_raises(Protocol::ZMTP::Error) { Greeting.decode(bad) }
    end

    it "raises on ZMTP version < 3.0" do
      old = Greeting.encode.dup
      old.setbyte(10, 2)
      assert_raises(Protocol::ZMTP::Error) { Greeting.decode(old) }
    end
  end
end
