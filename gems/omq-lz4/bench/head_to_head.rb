# frozen_string_literal: true

# Head-to-head: omq-lz4 vs omq-zstd.
#
# Two measurement layers:
# 1. Raw codec latency (compress + decompress) — isolates the
#    compression cost, no transport / socket / Async overhead.
# 2. End-to-end PUSH → PULL throughput over loopback — reflects what a
#    real OMQ application experiences.
#
# Both layers are run with and without a shared dictionary.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/head_to_head.rb

$stdout.sync = true

require "benchmark"
require "securerandom"
require "async"

require "lz4rip"
require "zrip"
require "omq"
require "omq/lz4"
require "omq/zstd"

LOREM = <<~TXT.tr("\n", " ").strip
  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
  tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam,
  quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo
  consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
  cillum dolore eu fugiat nulla pariatur.
TXT


# Lorem ipsum with a fresh UUID (36-byte ASCII form) between each
# copy — breaks the degenerate "same sentence repeated N times"
# case where LZ4 finds one back-reference covering the rest of the
# payload. UUIDs are incompressible, so at scale the wire size
# scales linearly with plaintext (UUID density ≈ 36 / (36 + 446)
# ≈ 7.5% of input is mandatory literal). Approximates realistic
# workloads where a schema repeats but values vary (event logs,
# protobuf records, etc.).
def payload_of(size)
  out = String.new(capacity: size + LOREM.bytesize + 64, encoding: Encoding::BINARY)
  while out.bytesize < size
    out << SecureRandom.uuid.b << LOREM
  end
  out.byteslice(0, size)
end


DICT_BYTES = (LOREM * 2).byteslice(0, 256).b


def round_trip(label, sizes, iters_hint, &block)
  puts
  puts "### #{label}"
  puts "  size     compress      decompress    round-trip    wire    ratio"
  puts "-" * 72
  sizes.each do |size|
    pt = payload_of(size)
    iters = [iters_hint, size >= 65_536 ? 2_000 : iters_hint].min
    bench = block.call(pt, iters)
    printf("  %7d  %8.3f µs  %8.3f µs   %8.3f µs   %6d   %.3f\n",
           size, bench[:c_us], bench[:d_us], bench[:rt_us], bench[:wire], bench[:ratio])
  end
end


def measure(iters, ct_slot: nil)
  # Cold-pass validation.
  first_compress  = yield(:compress)
  first_decompress = yield(:decompress, first_compress)

  # Warm up a little.
  1_000.times do
    yield(:compress)
    yield(:decompress, first_compress)
  end

  # compress-only
  dt_c = Benchmark.realtime do
    iters.times { yield(:compress) }
  end

  # decompress-only (we have first_compress on hand)
  dt_d = Benchmark.realtime do
    iters.times { yield(:decompress, first_compress) }
  end

  # round-trip
  dt_rt = Benchmark.realtime do
    iters.times do
      ct = yield(:compress)
      yield(:decompress, ct)
    end
  end

  {
    c_us:  dt_c  / iters * 1e6,
    d_us:  dt_d  / iters * 1e6,
    rt_us: dt_rt / iters * 1e6,
    wire:  first_compress.bytesize,
    pt:    first_decompress,
  }
end


def ratio(wire, pt_size)
  wire.to_f / [pt_size, 1].max
end


puts "omq-lz4 v#{OMQ::LZ4::VERSION}  (lz4rip v#{Lz4rip::VERSION})"
puts "omq-zstd v#{OMQ::Zstd::VERSION}  (zrip v#{Zrip::VERSION})"
puts "YJIT: #{RubyVM::YJIT.enabled? ? "on" : "off"}"
puts "Dict: #{DICT_BYTES.bytesize} B (Lorem ipsum prefix)"
puts "Input: dict-friendly text (repeating Lorem ipsum)"
puts "=" * 72


# --- Layer 1: raw codec latency ---

SIZES_MICRO = [64, 256, 1024, 16_384]

lz4_codec      = Lz4rip::BlockCodec.new
lz4_dict_codec = Lz4rip::BlockCodec.new(dict: DICT_BYTES)

zstd_codec      = Zrip::FrameCodec.new(level: -3)
zstd_dict_codec = Zrip::FrameCodec.new(dict: DICT_BYTES, level: -3)


round_trip("omq-lz4 BlockCodec (no dict)", SIZES_MICRO, 20_000) do |pt, iters|
  res = measure(iters) do |op, arg = nil|
    case op
    when :compress   then lz4_codec.compress(pt)
    when :decompress then lz4_codec.decompress(arg, decompressed_size: pt.bytesize)
    end
  end
  res[:ratio] = ratio(res[:wire], pt.bytesize)
  res
end


round_trip("omq-lz4 BlockCodec + dict", SIZES_MICRO, 20_000) do |pt, iters|
  res = measure(iters) do |op, arg = nil|
    case op
    when :compress   then lz4_dict_codec.compress(pt)
    when :decompress then lz4_dict_codec.decompress(arg, decompressed_size: pt.bytesize)
    end
  end
  res[:ratio] = ratio(res[:wire], pt.bytesize)
  res
end


round_trip("omq-zstd FrameCodec level=-3 (no dict)", SIZES_MICRO, 20_000) do |pt, iters|
  res = measure(iters) do |op, arg = nil|
    case op
    when :compress   then zstd_codec.compress(pt)
    when :decompress then zstd_codec.decompress(arg)
    end
  end
  res[:ratio] = ratio(res[:wire], pt.bytesize)
  res
end


round_trip("omq-zstd FrameCodec level=-3 + dict", SIZES_MICRO, 20_000) do |pt, iters|
  res = measure(iters) do |op, arg = nil|
    case op
    when :compress   then zstd_dict_codec.compress(pt)
    when :decompress then zstd_dict_codec.decompress(arg)
    end
  end
  res[:ratio] = ratio(res[:wire], pt.bytesize)
  res
end


# --- Layer 2: end-to-end transport throughput ---

puts
puts "=" * 72
puts "End-to-end throughput (PUSH → PULL, loopback)"
puts "=" * 72

SIZES_E2E      = ENV["SIZES"] ? ENV["SIZES"].split(",").map(&:to_i) :
                 [256, 1024, 16_384, 32_768, 65_536, 131_072, 262_144, 524_288]
N              = Integer(ENV["N"]      || 20_000)
WARMUP_PER_RUN = Integer(ENV["WARMUP"] ||  2_000)


def run_transport(scheme, size:, dict: nil, **extras)
  pt = payload_of(size)
  Sync do |task|
    pull = OMQ::PULL.new
    push = OMQ::PUSH.new
    uri  = pull.bind("#{scheme}://127.0.0.1:0", dict: dict, **extras)
    push.connect(uri.to_s, dict: dict, **extras)
    push.peer_connected.wait

    # Pipeline by running send and receive on separate fibers.
    # A "send all then receive all" loop deadlocks for payloads that
    # exceed HWM × msg_size + TCP kernel buffer — e.g. 16 KiB × 2 000
    # = 32 MiB, well past the ~17 MiB send-queue + loopback-buffer
    # budget.

    # Warm-up
    warm_send = task.async { WARMUP_PER_RUN.times { push << [pt] } }
    WARMUP_PER_RUN.times { pull.receive }
    warm_send.wait

    # Measured phase
    t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sender = task.async { N.times { push << [pt] } }
    N.times { pull.receive }
    sender.wait
    dt = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t
    { msg_s: (N / dt).round, us_rt: (dt / N * 1e6).round(2) }
  ensure
    push&.close
    pull&.close
  end
end


MIB = 1024.0 * 1024.0

ZSTD_MIN_COMPRESS_NO_DICT = OMQ::Transport::ZstdTcp::Codec::MIN_COMPRESS_NO_DICT
LZ4_BLOCK_CODEC   = Lz4rip::BlockCodec.new
ZSTD_FRAME_CODECS = Hash.new { |h, level| h[level] = Zrip::FrameCodec.new(level: level) }

# Sample wire-bytes-per-message for each transport — the actual
# bytes the socket sees per user message (ignoring ZMTP frame
# header overhead, ~9 B per part).
def wire_bytes_per_msg(scheme, plaintext, level: -3)
  case scheme
  when "tcp"
    plaintext.bytesize
  when "lz4+tcp"
    OMQ::LZ4::Codec.encode_part(plaintext, block_codec: LZ4_BLOCK_CODEC).bytesize
  when "zstd+tcp"
    # Mirrors OMQ::Transport::ZstdTcp::Codec#compress_or_plain:
    # - below the no-dict threshold → NUL_PREAMBLE (4 B) + plaintext
    # - compressed doesn't save ≥ 4 B → same passthrough
    # - otherwise → raw zstd frame, no extra sentinel
    return 4 + plaintext.bytesize if plaintext.bytesize < ZSTD_MIN_COMPRESS_NO_DICT
    compressed = ZSTD_FRAME_CODECS[level].compress(plaintext)
    return 4 + plaintext.bytesize if compressed.bytesize >= plaintext.bytesize - 4
    compressed.bytesize
  end
end


def show(label, size, r, wire_per_msg:, baseline_plain_mib: nil)
  plain_mib = (size * r[:msg_s]) / MIB
  wire_mib  = (wire_per_msg * r[:msg_s]) / MIB
  speedup   = baseline_plain_mib ? " (%.2fx vs tcp)" % (plain_mib / baseline_plain_mib) : ""
  printf("  %-38s %9d msg/s  %7.2f µs/RT  plain=%7.1f MiB/s  wire=%7.1f MiB/s%s\n",
         label, r[:msg_s], r[:us_rt], plain_mib, wire_mib, speedup)
  plain_mib
end


# Note on dict paths: omq-lz4 accepts raw dict bytes; omq-zstd's
# install_send_dict requires ZDICT-format (trained or zstd-native).
# An apples-to-apples transport dict comparison would need separate
# dicts per codec. Microbench above already measures dict vs no-dict
# on both codecs with raw bytes — that covers the compression-quality
# story. Transport-layer bench stays no-dict; it isolates the fixed
# transport overhead (sentinel dispatch, I/O, Async scheduling).
SIZES_E2E.each do |size|
  pt = payload_of(size)
  puts
  puts "Payload: #{size} bytes"

  tcp_wire       = wire_bytes_per_msg("tcp",      pt)
  lz4_wire       = wire_bytes_per_msg("lz4+tcp",  pt)
  zstd_neg3_wire = wire_bytes_per_msg("zstd+tcp", pt, level: -3)
  zstd_pos1_wire = wire_bytes_per_msg("zstd+tcp", pt, level:  1)
  zstd_pos3_wire = wire_bytes_per_msg("zstd+tcp", pt, level:  3)

  baseline = show("tcp (baseline, no compression)", size,
                  run_transport("tcp", size: size),
                  wire_per_msg: tcp_wire)
  show("lz4+tcp (no dict)", size,
       run_transport("lz4+tcp", size: size),
       wire_per_msg: lz4_wire, baseline_plain_mib: baseline)
  show("zstd+tcp level=-3 (no dict)", size,
       run_transport("zstd+tcp", size: size, level: -3),
       wire_per_msg: zstd_neg3_wire, baseline_plain_mib: baseline)
  show("zstd+tcp level= 1 (no dict)", size,
       run_transport("zstd+tcp", size: size, level:  1),
       wire_per_msg: zstd_pos1_wire, baseline_plain_mib: baseline)
  show("zstd+tcp level= 3 (no dict)", size,
       run_transport("zstd+tcp", size: size, level:  3),
       wire_per_msg: zstd_pos3_wire, baseline_plain_mib: baseline)
end
