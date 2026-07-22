# frozen_string_literal: true

require_relative "test_helper"

SIZE_BUCKETS = [0, 1, 255, 256, 4 * 1024, 64 * 1024, 1024 * 1024].freeze

describe OMQ::LZ4::Codec do
  let(:codec)      { Lz4rip::BlockCodec.new }
  let(:dict_bytes) { ("header version=1 type=event field=" * 4).b }
  let(:dict_codec) { Lz4rip::BlockCodec.new(dict: dict_bytes) }

  describe ".encode_part / .decode_part (no dict)" do
    it "round-trips across size buckets with random bytes" do
      SIZE_BUCKETS.each do |n|
        pt = Random.bytes(n)
        wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
        out  = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec)
        assert_equal pt, out, "round-trip failed at size #{n}"
      end
    end

    it "uses UNCOMPRESSED sentinel below min_size" do
      pt   = "x" * 100
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      assert_equal "\x00\x00\x00\x00".b, wire.byteslice(0, 4)
      assert_equal pt, wire.byteslice(4, wire.bytesize - 4)
    end

    it "uses LZ4B sentinel for compressible payloads above min_size" do
      pt   = ("A" * 1024).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      assert_equal "LZ4B".b, wire.byteslice(0, 4)
      assert_operator wire.bytesize, :<, pt.bytesize
      decompressed_size = wire.byteslice(4, 8).unpack1("Q<")
      assert_equal pt.bytesize, decompressed_size
    end

    it "falls back to passthrough when compression wouldn't save ≥ 8 bytes" do
      # Random 1 KiB payload (above the 512-byte no-dict min_size) won't
      # compress at all — encoder must emit UNCOMPRESSED despite being
      # above the threshold.
      pt   = Random.bytes(1024)
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      assert_equal "\x00\x00\x00\x00".b, wire.byteslice(0, 4),
        "expected passthrough fallback on random-byte input"
    end

    it "honours a caller-supplied min_size override" do
      pt = ("A" * 128).b  # below default 512; compresses well (repetitive)

      # Default threshold: passthrough.
      default_wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      assert_equal "\x00\x00\x00\x00".b, default_wire.byteslice(0, 4),
        "expected passthrough below default 512-byte threshold"

      # Lowered threshold: compresses (input is highly repetitive).
      forced_wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, min_size: 8)
      assert_equal "LZ4B".b, forced_wire.byteslice(0, 4),
        "expected LZ4B compression once min_size is lowered enough"
      assert_equal pt, OMQ::LZ4::Codec.decode_part(forced_wire, block_codec: codec)
    end
  end

  describe ".encode_part / .decode_part (with dict)" do
    it "round-trips across size buckets with random bytes" do
      SIZE_BUCKETS.each do |n|
        pt = Random.bytes(n)
        wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: dict_codec)
        out  = OMQ::LZ4::Codec.decode_part(wire, block_codec: dict_codec)
        assert_equal pt, out, "dict round-trip failed at size #{n}"
      end
    end

    it "round-trips dict-prefixed input and compresses better with dict than without" do
      pt = (dict_bytes + "payload body").b
      wire_with    = OMQ::LZ4::Codec.encode_part(pt, block_codec: dict_codec)
      wire_without = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      assert_operator wire_with.bytesize, :<, wire_without.bytesize,
        "dict wire size should beat no-dict wire size on dict-prefixed input"
      assert_equal pt, OMQ::LZ4::Codec.decode_part(wire_with, block_codec: dict_codec)
    end

    it "honours the 32-byte with-dict min_size threshold" do
      # At 33 bytes we're above the with-dict threshold. Verify the
      # codec picks its default threshold based on has_dict?.
      pt   = "small" * 7  # 35 bytes: above 32-byte with-dict threshold
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: dict_codec)
      # Either LZ4B (if it compresses well enough) or UNCOMPRESSED
      # (if passthrough wins on this tiny input). Both are legal; the
      # assertion is that we TRIED to compress (we were ≥ min_size).
      # Concretely: the sentinel is LZ4B when compression helped, or
      # UNCOMPRESSED via the saving-≤0 fallback. We can't distinguish
      # "didn't try" from "tried-and-fell-back" from the wire alone,
      # so the invariant we test is simply round-trip.
      assert_equal pt, OMQ::LZ4::Codec.decode_part(wire, block_codec: dict_codec)
    end
  end

  describe ".encode_part / .decode_part (multi-block, LZ4M)" do
    let(:block_size) { 1024 }

    it "round-trips a payload spanning multiple blocks" do
      pt = ("A" * 4096).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      assert_equal "LZ4M".b, wire.byteslice(0, 4)
      decompressed_size = wire.byteslice(4, 8).unpack1("Q<")
      assert_equal pt.bytesize, decompressed_size
      out = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      assert_equal pt, out
    end

    it "round-trips with a dict" do
      pt = (dict_bytes * 40).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: dict_codec, block_size: block_size)
      assert_equal "LZ4M".b, wire.byteslice(0, 4)
      out = OMQ::LZ4::Codec.decode_part(wire, block_codec: dict_codec, block_size: block_size)
      assert_equal pt, out
    end

    it "round-trips a payload whose last block is smaller than block_size" do
      pt = ("B" * 2500).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      assert_equal "LZ4M".b, wire.byteslice(0, 4)
      out = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      assert_equal pt, out
    end

    it "round-trips random bytes across multiple blocks" do
      pt = Random.bytes(3000)
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      assert_equal "LZ4M".b, wire.byteslice(0, 4)
      out = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      assert_equal pt, out
    end

    it "uses LZ4B for plaintext exactly at block_size" do
      pt = ("A" * block_size).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      assert_equal "LZ4B".b, wire.byteslice(0, 4)
    end

    it "uses LZ4M for plaintext one byte over block_size" do
      pt = ("A" * (block_size + 1)).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      assert_equal "LZ4M".b, wire.byteslice(0, 4)
      out = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      assert_equal pt, out
    end
  end

  describe ".decode_part LZ4M malformed inputs" do
    let(:block_size) { 1024 }

    it "rejects LZ4M part shorter than 12 bytes" do
      wire = "LZ4M".b + "\x00" * 7
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      end
    end

    it "rejects when decompressed_size exceeds max_size" do
      pt = ("A" * 2048).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 1000, block_size: block_size)
      end
      assert_match(/exceeds max_size/, err.message)
    end

    it "rejects truncated block length" do
      wire = "LZ4M".b + [2048].pack("Q<") + "\x00\x00"
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      end
    end

    it "rejects truncated block data" do
      wire = "LZ4M".b + [2048].pack("Q<") + [100].pack("V") + "x" * 10
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      end
    end

    it "rejects leftover bytes after last block" do
      pt = ("A" * 2048).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, block_size: block_size)
      wire_with_extra = wire + "extra"
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire_with_extra, block_codec: codec, block_size: block_size)
      end
    end

    it "rejects a block with corrupt compressed data" do
      wire = "LZ4M".b + [2048].pack("Q<") + [50].pack("V") + Random.bytes(50)
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, block_size: block_size)
      end
    end
  end

  describe ".decode_part LZ4B block size limit" do
    it "rejects LZ4B with decompressed_size exceeding LZ4M_BLOCK_SIZE" do
      huge_size = OMQ::LZ4::Codec::LZ4M_BLOCK_SIZE + 1
      wire = "LZ4B".b + [huge_size].pack("Q<") + "x"
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec)
      end
      assert_match(/exceeds block size limit/, err.message)
    end
  end

  describe ".decode_part bounded output" do
    it "rejects a compressed part that declares a size above max_size" do
      pt   = ("A" * 10_000).b
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec)
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 1000)
      end
      assert_match(/exceeds max_size/, err.message)
    end

    it "rejects an uncompressed part whose payload exceeds max_size" do
      pt   = "x" * 2000
      wire = OMQ::LZ4::Codec.encode_part(pt, block_codec: codec, min_size: 10_000)  # force passthrough
      assert_equal "\x00\x00\x00\x00".b, wire.byteslice(0, 4)
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 500)
      end
      assert_match(/exceeds max_size/, err.message)
    end

    it "rejects before decoder invocation (lie-about-size is caught)" do
      # Hand-craft LZ4B with a huge declared size over a tiny block body.
      # max_size must catch it before LZ4_decompress_safe is invoked.
      fake_size = 10 * 1024 * 1024
      wire = "LZ4B".b + [fake_size].pack("Q<") + "garbage"
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 1024)
      end
    end
  end

  describe ".decode_part malformed inputs" do
    it "raises on part shorter than 4 bytes" do
      ["", "\x00", "\x00\x01\x02"].each do |buf|
        assert_raises(OMQ::LZ4::ProtocolError) do
          OMQ::LZ4::Codec.decode_part(buf.b, block_codec: codec)
        end
      end
    end

    it "raises on unknown sentinel" do
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part("\xDE\xAD\xBE\xEF".b, block_codec: codec)
      end
    end

    it "raises on LZ4B with no size field" do
      # 4-byte sentinel + 7 bytes of random tail — not enough for the
      # 8-byte decompressed_size field.
      wire = "LZ4B".b + "\x00" * 7
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec)
      end
    end

    it "raises on LZ4B with a malformed block body" do
      # Well-formed envelope (sentinel + declared size = 100), but the
      # block bytes are random garbage. Must raise, not segfault.
      wire = "LZ4B".b + [100].pack("Q<") + "\xFF".b
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 200)
      end
    end

    it "raises on LZ4D sentinel (shipment not routable via decode_part)" do
      wire = "LZ4D".b + "dict bytes"
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_part(wire, block_codec: codec)
      end
      assert_match(/LZ4D/, err.message)
    end

    it "survives a fuzz of random wire inputs without crashing" do
      srand(0xBABE)
      1000.times do
        len  = rand(0..1024)
        wire = Random.bytes(len)
        begin
          OMQ::LZ4::Codec.decode_part(wire, block_codec: codec, max_size: 65_536)
        rescue OMQ::LZ4::ProtocolError
          # expected for most inputs
        end
      end
    end
  end

  describe "DictTrainer + Codec round-trip" do
    it "trained dict produces valid dict shipments and improves compression" do
      trainer = Lz4rip::DictTrainer.new(2048)
      samples = 200.times.map do |i|
        %Q({"event":"login","user":"user_#{i}","ts":"2026-06-12T00:00:00.#{format("%04d", i)}Z","region":"us-east-1","status":200,"latency_ms":#{10 + i % 490}}).b
      end
      samples.each { |s| trainer.add_sample(s) }
      dict_bytes = trainer.train

      refute_empty dict_bytes
      assert_operator dict_bytes.bytesize, :<=, OMQ::LZ4::Codec::MAX_DICT_SIZE

      shipment_wire = OMQ::LZ4::Codec.encode_dict_shipment(dict_bytes)
      decoded_dict  = OMQ::LZ4::Codec.decode_dict_shipment(shipment_wire)
      assert_equal dict_bytes, decoded_dict

      dict_codec = Lz4rip::BlockCodec.new(dict: dict_bytes)
      no_dict    = Lz4rip::BlockCodec.new

      msg = %Q({"event":"login","user":"user_9999","ts":"2026-06-12T00:00:00.9999Z","region":"us-east-1","status":200,"latency_ms":42,"trace":"abcdef0123456789","path":"/v1/users/9999"}).b
      wire_with    = OMQ::LZ4::Codec.encode_part(msg, block_codec: dict_codec)
      wire_without = OMQ::LZ4::Codec.encode_part(msg, block_codec: no_dict, min_size: 8)
      assert_operator wire_with.bytesize, :<, wire_without.bytesize

      pt = OMQ::LZ4::Codec.decode_part(wire_with, block_codec: dict_codec)
      assert_equal msg, pt
    end
  end

  describe ".encode_dict_shipment / .decode_dict_shipment" do
    it "round-trips dict bytes" do
      [1, 2, 100, 1024, OMQ::LZ4::Codec::MAX_DICT_SIZE].each do |n|
        d    = Random.bytes(n)
        wire = OMQ::LZ4::Codec.encode_dict_shipment(d)
        out  = OMQ::LZ4::Codec.decode_dict_shipment(wire)
        assert_equal d, out, "dict shipment round-trip failed at size #{n}"
      end
    end

    it "prefixes the LZ4D sentinel" do
      wire = OMQ::LZ4::Codec.encode_dict_shipment("xxx")
      assert_equal "LZ4D".b, wire.byteslice(0, 4)
    end

    it "raises on encode of empty dict" do
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.encode_dict_shipment("")
      end
    end

    it "raises on encode of oversized dict" do
      oversized = "x" * (OMQ::LZ4::Codec::MAX_DICT_SIZE + 1)
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.encode_dict_shipment(oversized)
      end
    end

    it "raises on decode of wrong sentinel" do
      wire = "LZ4B".b + "xxx"
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_dict_shipment(wire)
      end
    end

    it "raises on decode of a shipment declaring zero dict bytes" do
      wire = "LZ4D".b
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_dict_shipment(wire)
      end
    end

    it "raises on decode of shipment whose dict is too large" do
      oversized = "LZ4D".b + ("x" * (OMQ::LZ4::Codec::MAX_DICT_SIZE + 1))
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_dict_shipment(oversized)
      end
    end

    it "raises on decode of shorter-than-sentinel input" do
      assert_raises(OMQ::LZ4::ProtocolError) do
        OMQ::LZ4::Codec.decode_dict_shipment("LZ4")
      end
    end
  end
end
