# frozen_string_literal: true

require_relative "../test_helper"
require "stringio"

describe Protocol::ZMTP::Codec::Frame do
  Frame = Protocol::ZMTP::Codec::Frame

  def stream(data)
    IO::Stream::Buffered.new(StringIO.new(data))
  end

  def roundtrip(frame)
    Frame.read_from(stream(frame.to_wire))
  end

  describe "#to_wire and .read_from round-trip" do
    it "handles empty body" do
      frame = Frame.new("".b)
      decoded = roundtrip(frame)
      assert_equal "".b, decoded.body
      refute decoded.more?
      refute decoded.command?
    end

    it "handles short body (< 256 bytes)" do
      body = "hello world".b
      frame = Frame.new(body)
      wire = frame.to_wire
      # short frame: 1 byte flags + 1 byte size + body
      assert_equal 2 + body.bytesize, wire.bytesize
      decoded = roundtrip(frame)
      assert_equal body, decoded.body
    end

    it "handles body at short frame boundary (255 bytes)" do
      body = ("x" * 255).b
      frame = Frame.new(body)
      wire = frame.to_wire
      assert_equal 2 + 255, wire.bytesize
      decoded = roundtrip(frame)
      assert_equal body, decoded.body
    end

    it "handles long body (> 255 bytes)" do
      body = ("A" * 256).b
      frame = Frame.new(body)
      wire = frame.to_wire
      # long frame: 1 byte flags + 8 byte size + body
      assert_equal 9 + 256, wire.bytesize
      decoded = roundtrip(frame)
      assert_equal body, decoded.body
    end

    it "preserves MORE flag" do
      frame = Frame.new("data".b, more: true)
      decoded = roundtrip(frame)
      assert decoded.more?
      refute decoded.command?
    end

    it "preserves COMMAND flag" do
      frame = Frame.new("cmd".b, command: true)
      decoded = roundtrip(frame)
      assert decoded.command?
      refute decoded.more?
    end

    it "preserves MORE + COMMAND flags together" do
      frame = Frame.new("data".b, more: true, command: true)
      decoded = roundtrip(frame)
      assert decoded.more?
      assert decoded.command?
    end
  end

  describe "#to_wire encoding" do
    it "sets flags byte correctly for short frame" do
      frame = Frame.new("x".b, more: true)
      wire = frame.to_wire
      flags = wire.getbyte(0)
      assert_equal Frame::FLAGS_MORE, flags & Frame::FLAGS_MORE
      assert_equal 0, flags & Frame::FLAGS_LONG
    end

    it "sets LONG flag for large frames" do
      frame = Frame.new(("x" * 256).b)
      wire = frame.to_wire
      flags = wire.getbyte(0)
      assert_equal Frame::FLAGS_LONG, flags & Frame::FLAGS_LONG
    end

    it "encodes size as big-endian uint64 for long frames" do
      body = ("A" * 300).b
      frame = Frame.new(body)
      wire = frame.to_wire
      size = wire.byteslice(1, 8).unpack1("Q>")
      assert_equal 300, size
    end
  end

  describe ".read_from" do
    it "raises EOFError on empty IO" do
      assert_raises(EOFError) { Frame.read_from(stream("".b)) }
    end

    it "raises EOFError on truncated frame" do
      wire = [0x00, 10].pack("CC") + ("x" * 5)
      assert_raises(EOFError) { Frame.read_from(stream(wire)) }
    end

    it "raises before reading body when frame exceeds max_message_size" do
      # Header only: LONG flag + 8-byte size claiming 1 GB, no body bytes.
      # With the old code, read_from would call read_exactly(1GB) and raise
      # EOFError. With the fix, it raises Error before touching the body.
      header = [0x02].pack("C") + [1_000_000_000].pack("Q>")
      io     = stream(header)
      err    = assert_raises(Protocol::ZMTP::Error) do
        Frame.read_from(io, max_message_size: 100)
      end
      assert_match(/exceeds max_message_size/, err.message)
    end

    it "raises for oversized command frames too" do
      # Command flag set — must still be rejected
      header = [0x06].pack("C") + [1_000_000_000].pack("Q>") # LONG | COMMAND
      io     = stream(header)
      assert_raises(Protocol::ZMTP::Error) do
        Frame.read_from(io, max_message_size: 100)
      end
    end

    it "allows frames within max_message_size" do
      body  = "hello".b
      frame = Frame.new(body)
      wire  = frame.to_wire
      decoded = Frame.read_from(stream(wire), max_message_size: 100)
      assert_equal body, decoded.body
    end
  end

  describe ".encode_message" do
    it "encodes a single-part message" do
      wire = Frame.encode_message(["hello"])
      f    = Frame.read_from(stream(wire))
      assert_equal "hello", f.body
      refute f.more?
    end

    it "encodes a multi-part message with correct MORE flags" do
      wire = Frame.encode_message(["part1", "part2", "part3"])
      io   = stream(wire)

      f1 = Frame.read_from(io)
      assert_equal "part1", f1.body
      assert f1.more?

      f2 = Frame.read_from(io)
      assert_equal "part2", f2.body
      assert f2.more?

      f3 = Frame.read_from(io)
      assert_equal "part3", f3.body
      refute f3.more?
    end

    it "returns a frozen string" do
      assert Frame.encode_message(["data"]).frozen?
    end

    it "produces identical bytes to per-frame encoding" do
      parts   = ["topic.foo", "payload here"]
      encoded = Frame.encode_message(parts)

      manual = +""
      parts.each_with_index do |part, i|
        manual << Frame.new(part, more: i < parts.size - 1).to_wire
      end

      assert_equal manual, encoded
    end
  end
end
