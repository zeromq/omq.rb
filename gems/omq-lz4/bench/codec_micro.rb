# frozen_string_literal: true

# Microbenchmark: OMQ::LZ4::Codec.encode_part / decode_part.
# Measures per-message latency and compression ratio across the size
# buckets called out in the plan (M5 Polish): 64 B, 256 B, 1 KiB,
# 16 KiB, 1 MiB. With and without a dictionary.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/codec_micro.rb

require "benchmark"
require "omq/lz4"

LOREM = <<~TXT.tr("\n", " ").strip
  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
  tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,
  quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
  consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
  cillum dolore eu fugiat nulla pariatur.
TXT

def payload_of(size)
  (LOREM * ((size / LOREM.bytesize) + 1)).byteslice(0, size).b
end

DICT_BYTES = (LOREM * 2).byteslice(0, 512).b
SIZES      = [64, 256, 1024, 16_384, 1_048_576]

def bench(plaintext, codec)
  # Ensure encode_part is producing round-trippable output.
  wire = OMQ::LZ4::Codec.encode_part(plaintext, block_codec: codec, min_size: 0)
  out  = OMQ::LZ4::Codec.decode_part(wire, block_codec: codec)
  raise "round-trip mismatch at size #{plaintext.bytesize}" unless out == plaintext

  # Latency: cap iterations so the 1 MiB bucket doesn't take forever.
  iters = plaintext.bytesize >= 65_536 ? 2_000 : 50_000

  dt_c = Benchmark.realtime do
    iters.times { OMQ::LZ4::Codec.encode_part(plaintext, block_codec: codec, min_size: 0) }
  end
  dt_d = Benchmark.realtime do
    iters.times { OMQ::LZ4::Codec.decode_part(wire, block_codec: codec) }
  end

  {
    wire_size: wire.bytesize,
    ratio:     wire.bytesize.to_f / [plaintext.bytesize, 1].max,
    c_ns:      (dt_c / iters * 1e9).round,
    d_ns:      (dt_d / iters * 1e9).round,
    iters:     iters,
  }
end


def fmt(label, pt, row)
  "  %-32s  in=%8d  wire=%8d  ratio=%.3f  c=%7d ns  d=%7d ns  (%d iters)" %
    [label, pt.bytesize, row[:wire_size], row[:ratio], row[:c_ns], row[:d_ns], row[:iters]]
end


puts "omq-lz4 v#{OMQ::LZ4::VERSION}"
puts "lz4rip v#{Lz4rip::VERSION}"
puts "dict_bytes=#{DICT_BYTES.bytesize}"
puts "YJIT: #{RubyVM::YJIT.enabled? ? "on" : "off"}"
puts "-" * 100

no_dict_codec = Lz4rip::BlockCodec.new
dict_codec    = Lz4rip::BlockCodec.new(dict: DICT_BYTES)

SIZES.each do |size|
  pt = payload_of(size)
  puts
  puts "Payload: #{size} bytes"
  puts fmt("LZ4B (no dict)", pt, bench(pt, no_dict_codec))
  puts fmt("LZ4B + dict",    pt, bench(pt, dict_codec))
end
