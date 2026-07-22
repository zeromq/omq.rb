# frozen_string_literal: true

# Sweep the minimum input size where LZ4 block compression actually
# saves bytes over passthrough, for dict-friendly Lorem Ipsum input.
# Reports the byte-delta at each size so the threshold in
# OMQ::LZ4::Codec (currently 256 B no-dict, 64 B with-dict) can be
# empirically tuned.
#
# Crossover rule (from OMQ::LZ4::Codec.encode_part):
#   passthrough wins when compressed.bytesize + 12 >= plaintext.bytesize + 4
#   i.e. compression must save >= 8 bytes.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/min_compress_size_sweep.rb

require "lz4rip"

LOREM = <<~TXT.tr("\n", " ").strip
  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
  tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,
  quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
  consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
  cillum dolore eu fugiat nulla pariatur.
TXT

DICT_BYTES = (LOREM * 2).byteslice(0, 256).b

def payload_of(size)
  (LOREM * ((size / LOREM.bytesize) + 1)).byteslice(0, size).b
end


# Crossover semantics mirror OMQ::LZ4::Codec.encode_part:
#   LZ4B envelope     = 12 bytes (4 sentinel + 8 decompressed_size u64)
#   UNCOMPRESSED envelope = 4 bytes (sentinel)
# so compression "wins" when compressed_size < plaintext_size − 8.
ENVELOPE_DELTA = 12 - 4


def wins?(plaintext_size, compressed_size)
  compressed_size + 12 < plaintext_size + 4
end


def sweep(label, codec)
  puts
  puts "=== #{label} ==="
  puts "  N  plaintext  compressed   savings   wins?   wire (passthrough / LZ4B+env)"
  puts "-" * 80

  first_win = nil

  (8..320).step(4) do |n|
    pt = payload_of(n)
    ct = codec.compress(pt)

    savings = pt.bytesize - ct.bytesize
    won     = wins?(pt.bytesize, ct.bytesize)
    first_win ||= n if won

    passthrough_wire = pt.bytesize + 4
    lz4b_wire        = ct.bytesize + 12
    best             = [passthrough_wire, lz4b_wire].min

    marker = won ? "✓" : " "
    puts "  %4d  %7d    %7d   %+6d  %s       %4d / %4d   (best=%4d)" %
      [n, pt.bytesize, ct.bytesize, savings, marker, passthrough_wire, lz4b_wire, best]
  end

  puts
  if first_win
    puts "  → first size where compression saves ≥ 1 byte over passthrough (after envelope): #{first_win} B"
  else
    puts "  → no size in the sweep range yielded a win"
  end
end


puts "omq-lz4 min-compress-size sweep, lz4rip v#{Lz4rip::VERSION}"
puts "Lorem ipsum prefix, LZ4 default acceleration"
puts "Crossover: compressed_size + 12 < plaintext_size + 4"

sweep("NO DICT",   Lz4rip::BlockCodec.new)
sweep("WITH DICT", Lz4rip::BlockCodec.new(dict: DICT_BYTES))
