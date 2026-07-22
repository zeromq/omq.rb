# frozen_string_literal: true

# End-to-end throughput benchmark: PUSH → PULL over lz4+tcp:// on
# loopback, single-part messages, with and without a shared dict.
# Reports messages per second and µs per round-trip.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/transport_throughput.rb

require "async"
require "omq"
require "omq/lz4"

LOREM = <<~TXT.tr("\n", " ").strip
  Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
  tempor incididunt ut labore et dolore magna aliqua.
TXT

DICT_BYTES = (LOREM * 2).byteslice(0, 256).b
SIZES      = [64, 256, 1024, 16_384]
N          = 20_000


def run(size:, dict: nil, auto_dict: nil)
  pt = (LOREM * ((size / LOREM.bytesize) + 1)).byteslice(0, size).b
  result = {}
  warm = auto_dict ? 200 : 100

  Sync do |task|
    pull = OMQ::PULL.new
    push = OMQ::PUSH.new
    uri  = pull.bind("lz4+tcp://127.0.0.1:0", dict: dict)
    push.connect(uri.to_s, dict: dict, auto_dict: auto_dict)

    # Warm-up (concurrent to avoid TCP buffer deadlock when dict
    # shipment triggers a flush mid-stream).
    s = task.async { warm.times { push << [pt] } }
    r = task.async { warm.times { pull.receive } }
    s.wait; r.wait

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    s = task.async { N.times { push << [pt] } }
    r = task.async { N.times { pull.receive } }
    s.wait; r.wait
    t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    result[:seconds]   = t1 - t0
    result[:msg_per_s] = (N / (t1 - t0)).round
    result[:us_per_rt] = ((t1 - t0) / N * 1e6).round(3)
  ensure
    push&.close
    pull&.close
  end

  result
end


def fmt(label, size, r)
  "  %-20s  size=%6d B  %8d msg/s  %.3f µs/RT  %.1f MiB/s" %
    [label, size, r[:msg_per_s], r[:us_per_rt],
     (size * r[:msg_per_s] / 1024.0 / 1024.0).round(1)]
end


puts "omq-lz4 v#{OMQ::LZ4::VERSION}"
puts "YJIT: #{RubyVM::YJIT.enabled? ? "on" : "off"}"
puts "#{N} messages per cell (after 100-message warm-up)"
puts "-" * 100

SIZES.each do |size|
  puts
  puts "Payload: #{size} bytes"
  puts fmt("lz4+tcp (no dict)",  size, run(size: size))
  puts fmt("lz4+tcp + dict",     size, run(size: size, dict: DICT_BYTES))
  puts fmt("lz4+tcp + auto_dict", size, run(size: size, auto_dict: true))
end
