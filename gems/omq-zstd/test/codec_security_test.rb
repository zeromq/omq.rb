# frozen_string_literal: true

require_relative "test_helper"
require "zrip"

describe "Codec security" do
  # Shared frame codec for the tests that need to produce valid
  # frames on the fly. Zrip exposes persistent codec objects rather
  # than module-level compression helpers.
  SEC_TEST_CODEC = Zrip::FrameCodec.new(level: -3)

  def codec(**opts)
    OMQ::Transport::ZstdTcp::Codec.new(level: -3, **opts)
  end

  def train_dict(samples, capacity:)
    trainer = Zrip::DictTrainer.new(capacity)
    samples.each { |s| trainer.add_sample(s) }
    Zrip::Dictionary.new(bytes: trainer.train).bytes
  end

  def connection(codec)
    OMQ::Transport::ZstdTcp::ZstdConnection.new(FakeConn.new, codec)
  end


  class FakeConn
    attr_reader :sent

    def initialize
      @sent = []
    end

    def write_message(parts)
      @sent << parts
    end

    def receive_message
      @sent.shift
    end

    def flush; end
  end


  it "rejects a frame whose declared FCS exceeds budget" do
    c = codec(max_message_size: 1_000)
    conn = connection(c)
    payload = "A" * 100_000
    frame   = SEC_TEST_CODEC.compress(payload)
    assert_raises(OMQ::Transport::ZstdTcp::ProtocolError) do
      conn.send(:decode_parts, [frame])
    end
  end


  it "allows a frame that fits the budget" do
    c = codec(max_message_size: 10_000)
    conn = connection(c)
    payload = "A" * 8_000
    frame   = SEC_TEST_CODEC.compress(payload)
    decoded = conn.send(:decode_parts, [frame])
    assert_equal [payload], decoded
  end


  it "rejects a compressed frame without Frame_Content_Size" do
    c = codec
    conn = connection(c)
    raw_frame = [0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x00, 0x01, 0x00, 0x00].pack("C*")
    assert_raises(OMQ::Transport::ZstdTcp::ProtocolError) do
      conn.send(:decode_parts, [raw_frame])
    end
  end


  it "rejects multipart message whose decompressed sum exceeds budget" do
    c = codec(max_message_size: 10_000)
    conn = connection(c)
    part_a = SEC_TEST_CODEC.compress("A" * 8_000)
    part_b = SEC_TEST_CODEC.compress("B" * 8_000)
    assert_raises(OMQ::Transport::ZstdTcp::ProtocolError) do
      conn.send(:decode_parts, [part_a, part_b])
    end
  end


  it "charges uncompressed sentinel against budget" do
    c = codec(max_message_size: 1_000)
    conn = connection(c)
    body = OMQ::Transport::ZstdTcp::Codec::NUL_PREAMBLE + ("x" * 20_000)
    assert_raises(OMQ::Transport::ZstdTcp::ProtocolError) do
      conn.send(:decode_parts, [body])
    end
  end


  it "allows dict overwrite" do
    c = codec
    conn = connection(c)
    samples = 400.times.map { |i| "user_#{i}|key=#{i}|val=#{i * 7}" }
    dict_bytes = train_dict(samples, capacity: 8 * 1024)
    result1 = conn.send(:decode_parts, [dict_bytes])
    assert_nil result1
    result2 = conn.send(:decode_parts, [dict_bytes])
    assert_nil result2
  end


  it "rejects oversized dict" do
    c = codec
    conn = connection(c)
    samples = 400.times.map { |i| "user_#{i}|key=#{i}|val=#{i * 7}" }
    dict_bytes = train_dict(samples, capacity: 8 * 1024)
    padded = dict_bytes + ("\x00" * (65 * 1024))
    assert_raises(OMQ::Transport::ZstdTcp::ProtocolError) do
      conn.send(:decode_parts, [padded])
    end
  end


  it "re-ships dict on new connection after reconnect" do
    c = codec
    samples = 400.times.map { |i| "user_#{i}|key=#{i}|val=#{i * 7}" }
    dict_bytes = train_dict(samples, capacity: 8 * 1024)
    c.send(:install_send_dict, dict_bytes)

    # First TCP connection: dict shipped on first send
    fake1 = FakeConn.new
    conn1 = OMQ::Transport::ZstdTcp::ZstdConnection.new(fake1, c)
    conn1.write_message(["payload"])
    assert conn1.instance_variable_get(:@dict_shipped)
    assert_equal [dict_bytes], fake1.sent.first

    # Reconnect: new ZstdConnection on same shared Codec (same as wrap_connection on reconnect)
    fake2 = FakeConn.new
    conn2 = OMQ::Transport::ZstdTcp::ZstdConnection.new(fake2, c)
    refute conn2.instance_variable_get(:@dict_shipped), "new connection must start with dict_shipped=false"
    conn2.write_message(["payload"])
    assert conn2.instance_variable_get(:@dict_shipped)
    assert_equal 2, fake2.sent.size, "dict frame + data frame must both be sent on reconnect"
    assert_equal [dict_bytes], fake2.sent.first, "dict must be re-shipped on reconnected connection"
  end


  it "encodes Frame_Content_Size in every compressed frame (no dict)" do
    c = codec
    payload = "hello world " * 60  # 720 bytes, above MIN_COMPRESS_NO_DICT (512), compressible
    wire = c.send(:compress_or_plain, payload.b)
    assert_equal OMQ::Transport::ZstdTcp::Codec::ZSTD_MAGIC, wire.byteslice(0, 4),
                 "repetitive payload must produce a Zstd-compressed frame"
    fcs = c.parse_frame_content_size(wire)
    refute_nil fcs, "compressed frame must carry Frame_Content_Size"
    assert_equal payload.bytesize, fcs
  end


  it "encodes Frame_Content_Size in every compressed frame (with dict)" do
    samples = 300.times.map { |i| "record_#{i}:val=#{i * 3}:ok" }
    dict_bytes = train_dict(samples, capacity: 8 * 1024)
    c = codec(dict: dict_bytes)
    payload = "hello world " * 20  # 240 bytes, above MIN_COMPRESS_WITH_DICT (64), compressible
    wire = c.send(:compress_or_plain, payload.b)
    assert_equal OMQ::Transport::ZstdTcp::Codec::ZSTD_MAGIC, wire.byteslice(0, 4),
                 "payload must produce a Zstd-compressed frame even with a dict codec"
    fcs = c.parse_frame_content_size(wire)
    refute_nil fcs, "dict-bound compressed frame must carry Frame_Content_Size"
    assert_equal payload.bytesize, fcs
  end


  it "all compressed parts from compress_parts carry Frame_Content_Size" do
    c = codec
    parts = ["red " * 150, "blue " * 150]  # 600 and 750 bytes, highly compressible
    wires = c.compress_parts(parts)
    parts.zip(wires).each_with_index do |(orig, wire), idx|
      next unless wire.byteslice(0, 4) == OMQ::Transport::ZstdTcp::Codec::ZSTD_MAGIC
      fcs = c.parse_frame_content_size(wire)
      refute_nil fcs, "part #{idx}: compressed frame must carry Frame_Content_Size"
      assert_equal orig.bytesize, fcs
    end
  end
end
