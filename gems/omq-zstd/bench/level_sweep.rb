# frozen_string_literal: true

# Sweep Zstd compression levels (negative + low positive). Reports
# compress/decompress latency and ratio for each level on a few
# representative payload sizes, with and without a shared dictionary.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/level_sweep.rb

require "benchmark"
require "zrip"

LOREM = <<~TXT.tr("\n", " ").strip
  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
  tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,
  quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
  consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
  cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat
  non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
  Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium
  doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore
  veritatis et quasi architecto beatae vitae dicta sunt explicabo.
TXT

def payload_of(size)
  (LOREM * ((size / LOREM.bytesize) + 1)).byteslice(0, size).b
end

DICT_BYTES = (LOREM * 4).byteslice(0, 512).b

SIZES  = [64, 256, 1024, 4096, 16_384]
LEVELS = [-22, -10, -5, -3, -1, 1, 3, 5, 9]
ITERS  = 20_000

def bench(plaintext)
  compressed = nil
  dt_c = Benchmark.realtime { ITERS.times { compressed = yield(:compress, plaintext) } }
  decompressed = nil
  dt_d = Benchmark.realtime { ITERS.times { decompressed = yield(:decompress, compressed) } }
  raise "round-trip mismatch" unless decompressed == plaintext

  {
    out_size: compressed.bytesize,
    ratio:    compressed.bytesize.to_f / plaintext.bytesize,
    c_ns:     (dt_c / ITERS * 1e9).round,
    d_ns:     (dt_d / ITERS * 1e9).round,
  }
end

def fmt(label, row)
  "  %-26s  out=%5d  ratio=%.3f  c=%7d ns  d=%7d ns" %
    [label, row[:out_size], row[:ratio], row[:c_ns], row[:d_ns]]
end

puts "zrip v#{Zrip::VERSION rescue "?"}"
puts "#{ITERS} iterations per cell, dict=#{DICT_BYTES.bytesize} B"
puts "-" * 100

no_dict_codecs = LEVELS.to_h { |lvl| [lvl, Zrip::FrameCodec.new(level: lvl)] }
dict_codecs    = LEVELS.to_h { |lvl| [lvl, Zrip::FrameCodec.new(dict: DICT_BYTES, level: lvl)] }

SIZES.each do |size|
  pt = payload_of(size)
  puts
  puts "Payload: #{size} bytes"

  LEVELS.each do |lvl|
    codec = no_dict_codecs[lvl]
    row   = bench(pt) { |op, d| op == :compress ? codec.compress(d) : codec.decompress(d) }
    puts fmt("Zstd L#{lvl.to_s.rjust(3)} (no dict)", row)
  end

  LEVELS.each do |lvl|
    codec = dict_codecs[lvl]
    row   = bench(pt) { |op, d| op == :compress ? codec.compress(d) : codec.decompress(d) }
    puts fmt("Zstd L#{lvl.to_s.rjust(3)} + dict", row)
  end
end
