# frozen_string_literal: true

# How low can MIN_COMPRESS_WITH_DICT go?
#
# `encode_part` already does a size-based break-even check *after*
# compression (if compressed.bytesize >= size - 4, fall back to the
# uncompressed preamble path). So MIN_COMPRESS_WITH_DICT is purely a
# latency short-circuit: skip the compress attempt entirely when the
# payload is too small for compression to ever pay off on average.
#
# What we measure here:
#   1. hit rate  — fraction of payloads where dict.compress beats
#                  (plaintext - 4 bytes), i.e. where the attempt wins
#   2. attempt latency (ns) — per-frame cost of dict.compress
#   3. skip latency (ns)    — per-frame cost of the NUL_PREAMBLE
#                             fast path (string concat)
#   4. mean savings on hits — average bytes saved when the attempt wins
#
# From (1) and (4) you get expected wire-size savings per frame; from
# (2) and (3) you get the latency delta. The threshold is the smallest
# size where expected savings × throughput offsets the CPU cost on the
# hot path.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/min_compress_bytes_dict.rb

require "benchmark"
require "omq/zstd"

DEFAULT_LEVEL = -3

# Representative training corpus: the kind of chatty structured text
# you'd see on an omq control channel or log stream. Mix of JSON-ish,
# URL-ish, and prose tokens so the dict sees varied n-grams.
CORPUS = [
  %q({"type":"event","ts":1712000000,"level":"info","msg":"connected"}),
  %q({"type":"event","ts":1712000001,"level":"warn","msg":"retrying"}),
  %q({"type":"metric","name":"requests","value":42,"tags":["prod","us-east"]}),
  %q({"type":"metric","name":"latency_ms","value":7.3,"tags":["prod"]}),
  %q(GET /api/v1/users/42 HTTP/1.1 user-agent=omq/0.19),
  %q(POST /api/v1/orders HTTP/1.1 content-type=application/json),
  %q(tcp://worker-01.internal:5555 peer_connected),
  %q(tcp://worker-02.internal:5555 peer_connected),
  %q(ipc://@omq-control peer_connected),
  "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
  "The quick brown fox jumps over the lazy dog.",
  "Sed ut perspiciatis unde omnis iste natus error sit voluptatem.",
].freeze

# Train an auto dict from the corpus at level -3. Use enough
# samples that zrip's trainer has something to work with.
SAMPLES = (CORPUS * 200).map(&:b)
TRAINER = Zrip::DictTrainer.new(4096)
SAMPLES.each { |sample| TRAINER.add_sample(sample) }
DICT_BYTES      = TRAINER.train
DICT            = Zrip::FrameCodec.new(dict: DICT_BYTES, level: DEFAULT_LEVEL)
DICT_BYTES_SIZE = DICT_BYTES.bytesize

SENTINEL = OMQ::Transport::ZstdTcp::Codec::NUL_PREAMBLE
OVERHEAD = SENTINEL.bytesize
MIN_COMPRESS_WITH_DICT = OMQ::Transport::ZstdTcp::Codec::MIN_COMPRESS_WITH_DICT

# Workload generators. All return N distinct binary payloads of exactly
# `size` bytes.
WORKLOADS = {
  # Structured text matching the trained dict (friendly case).
  "matched text" => ->(size, n) {
    joined = (CORPUS.join(" ") * 20).b
    (0...n).map { |i| joined.byteslice((i * 17) % (joined.bytesize - size), size) }
  },

  # Unrelated English prose — no overlap with dict tokens.
  "mismatched prose" => ->(size, n) {
    src = ("The quick brown fox jumps over thirteen lazy dogs while " \
           "reading yesterday's newspaper under a flickering streetlamp. ").b * 20
    (0...n).map { |i| src.byteslice((i * 13) % (src.bytesize - size), size) }
  },

  # Pseudo-random bytes — incompressible.
  "random bytes" => ->(size, n) {
    rng = Random.new(42)
    (0...n).map { rng.bytes(size) }
  },

  # 50/50 mix of structured text and random bytes.
  "mixed binary" => ->(size, n) {
    rng = Random.new(7)
    text = (CORPUS.join(" ") * 20).b
    (0...n).map do |i|
      half = size / 2
      text.byteslice((i * 11) % (text.bytesize - half), half) + rng.bytes(size - half)
    end
  },
}

SIZES = [16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 256]
N_PAYLOADS = 200
ITERS = 5_000

def bench_attempt(payloads)
  dt = Benchmark.realtime do
    ITERS.times do
      payloads.each { |pt| DICT.compress(pt) }
    end
  end
  dt / (ITERS * payloads.size) * 1e9
end

def bench_skip(payloads)
  dt = Benchmark.realtime do
    ITERS.times do
      payloads.each { |pt| SENTINEL + pt }
    end
  end
  dt / (ITERS * payloads.size) * 1e9
end

puts "zrip v#{Zrip::VERSION rescue "?"}  dict=#{DICT_BYTES_SIZE}B level=#{DEFAULT_LEVEL}"
puts "#{N_PAYLOADS} distinct payloads/size, #{ITERS} iterations per cell"
puts "current MIN_COMPRESS_WITH_DICT = #{MIN_COMPRESS_WITH_DICT}"

WORKLOADS.each do |name, gen|
  puts
  puts "Workload: #{name}"
  printf "  %-6s  %-8s  %-12s  %-14s  %-12s  %-12s  %-10s\n",
    "size", "hit%", "mean_save", "expected_save", "attempt_ns", "skip_ns", "Δns/frame"
  puts "  " + "-" * 88

  SIZES.each do |size|
    payloads = gen.call(size, N_PAYLOADS)

    wins    = 0
    savings = 0
    payloads.each do |pt|
      c = DICT.compress(pt)
      if c.bytesize < pt.bytesize - OVERHEAD
        wins    += 1
        savings += (pt.bytesize - OVERHEAD) - c.bytesize
      end
    end
    hit_rate = wins.to_f / payloads.size
    mean_save_on_hit = wins.zero? ? 0.0 : savings.to_f / wins
    expected_save = hit_rate * mean_save_on_hit

    attempt_ns = bench_attempt(payloads)
    skip_ns    = bench_skip(payloads)

    printf "  %-6d  %-8s  %-12s  %-14s  %-12d  %-12d  %+d\n",
      size,
      "%.1f" % (hit_rate * 100),
      "%.1f B" % mean_save_on_hit,
      "%.2f B" % expected_save,
      attempt_ns.round,
      skip_ns.round,
      (attempt_ns - skip_ns).round
  end
end

puts
puts "Interpretation:"
puts "  hit%        — fraction of payloads where dict.compress wins vs SENTINEL+plaintext"
puts "  mean_save   — average bytes saved on winning attempts (at level -3)"
puts "  expected    — hit_rate × mean_save = average bytes saved per frame"
puts "  Δns/frame   — extra CPU per frame if we always attempt vs always skip"
puts "  break-even  — attempt worthwhile when expected_save outweighs the Δns cost"
puts "                at the target throughput (bytes/sec saved vs. ns/sec spent)"
