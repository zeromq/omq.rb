#!/usr/bin/env ruby
# frozen_string_literal: true

# PUB/SUB 2-process throughput benchmark with backend comparison.
#
# Spawns separate pub (sender) and sub (receiver) processes per cell,
# measures throughput and sender CPU, writes JSONL to ~/.cache/omq/ruby/.
#
# Usage:
#   ruby --yjit bench/bench_pubsub.rb                         # auto-detect backends
#   ruby --yjit bench/bench_pubsub.rb --backends ruby,rust    # specific backends
#   ruby --yjit bench/bench_pubsub.rb --quick                 # 3 sizes, 1 round
#   ruby --yjit bench/bench_pubsub.rb --chart                 # generate SVG after

$VERBOSE = nil
$stdout.sync = true

require "json"
require "fileutils"
require "timeout"
require_relative "chart_helper"

BENCH_PEER = File.join(__dir__, "bench_peer.rb")
RUBY_BIN   = RbConfig.ruby

CHART_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768].freeze
QUICK_SIZES = [128, 1024, 8192].freeze

DEFAULT_DURATION = 2.0
DEFAULT_ROUNDS   = 5
QUICK_DURATION   = 1.5
QUICK_ROUNDS     = 1

CELL_TIMEOUT = 10

CACHE_DIR  = File.join(ENV.fetch("XDG_CACHE_HOME", File.join(Dir.home, ".cache")), "omq", "ruby")
JSONL_PATH = File.join(CACHE_DIR, "results_pubsub.jsonl")


def parse_args
  opts = {
    backends: nil,
    sizes:    CHART_SIZES,
    duration: DEFAULT_DURATION,
    rounds:   DEFAULT_ROUNDS,
    chart:    false,
  }

  args = ARGV.dup
  while (arg = args.shift)
    case arg
    when "--backends"
      opts[:backends] = args.shift.split(",")
    when "--sizes"
      opts[:sizes] = args.shift.split(",").map(&:to_i)
    when "--duration"
      opts[:duration] = Float(args.shift)
    when "--rounds"
      opts[:rounds] = Integer(args.shift)
    when "--quick"
      opts[:sizes]    = QUICK_SIZES
      opts[:duration] = QUICK_DURATION
      opts[:rounds]   = QUICK_ROUNDS
    when "--chart"
      opts[:chart] = true
    end
  end

  opts[:backends] ||= detect_backends
  opts
end


def detect_backends
  available = ["ruby"]

  %w[rust ffi].each do |backend|
    lib = backend == "rust" ? "omq/rust" : "omq/ffi"
    check = IO.popen(
      [RUBY_BIN, "-e", "require 'bundler/setup'; require '#{lib}'; puts 'ok'"],
      err: File::NULL
    )
    output = check.read.strip
    check.close
    available << backend if output == "ok"
  end

  available
end


def peer_env(backend)
  { "OMQ_BENCH_BACKEND" => backend, "OMQ_DEV" => "1" }
end


def peer_cmd(mode, endpoint, msg_size, *extra_args)
  [RUBY_BIN, "--yjit", BENCH_PEER, mode, endpoint, msg_size.to_s] + extra_args.map(&:to_s)
end


def kill_peer(pid)
  Process.kill("KILL", pid)
  Process.wait(pid)
rescue Errno::ESRCH, Errno::ECHILD
end


def spawn_peer(mode, endpoint, msg_size, *extra_args, backend:)
  out_r, out_w = IO.pipe
  pid = Process.spawn(peer_env(backend), *peer_cmd(mode, endpoint, msg_size, *extra_args),
                      out: out_w, err: File::NULL)
  out_w.close
  [pid, out_r]
end


def run_cell(backend, msg_size, duration)
  pub_pid = nil
  sub_pid = nil
  pub_out = nil
  sub_out = nil

  Timeout.timeout(CELL_TIMEOUT) do
    pub_pid, pub_out = spawn_peer("pub", "tcp://127.0.0.1:0", msg_size, backend: backend)

    line = pub_out.gets
    port = line && line.strip.match(/^PORT (\d+)$/)&.[](1)&.to_i
    return nil unless port

    sleep 0.15

    sub_pid, sub_out = spawn_peer("sub", "tcp://127.0.0.1:#{port}", msg_size, duration.to_s, backend: backend)
    output = sub_out.read.strip
    sub_out.close
    sub_out = nil
    Process.wait(sub_pid)
    sub_pid = nil

    cpu_time = ChartHelper.read_proc_cpu(pub_pid)
    pub_out.close
    pub_out = nil
    kill_peer(pub_pid)
    pub_pid = nil

    return nil if output.empty?

    parts = output.split
    return nil if parts.size < 3

    count   = Integer(parts[0])
    elapsed = Float(parts[1])
    return nil if elapsed <= 0

    msgs_s = count.to_f / elapsed
    mbps   = count.to_f * msg_size / elapsed / 1_000_000.0

    {
      count:    count,
      elapsed:  elapsed,
      msgs_s:   msgs_s,
      mbps:     mbps,
      cpu_time: cpu_time,
    }
  end
rescue Timeout::Error
  nil
ensure
  pub_out&.close
  sub_out&.close
  kill_peer(pub_pid) if pub_pid
  kill_peer(sub_pid) if sub_pid
end


def main
  opts   = parse_args
  run_id = "ts-#{Time.now.to_i}"

  jit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
  puts "PUB/SUB | 2-process TCP loopback | Ruby #{RUBY_VERSION} (#{jit})"
  puts "run_id: #{run_id}"
  puts

  all_rows = []

  opts[:backends].each do |backend|
    puts "--- #{backend} backend ---"

    opts[:sizes].each do |size|
      best = nil

      opts[:rounds].times do
        cell = run_cell(backend, size, opts[:duration])
        if cell && (best.nil? || cell[:msgs_s] > best[:msgs_s])
          best = cell
        end
      end

      if best.nil?
        printf "  ~%6sB  FAILED\n", size
        next
      end

      cpu_pct = best[:elapsed] > 0 ? best[:cpu_time] / best[:elapsed] * 100 : 0

      printf "  ~%6sB  %9.0f msg/s  %7.2f MB/s  cpu %5.1f%%\n",
             size, best[:msgs_s], best[:mbps], cpu_pct

      all_rows << {
        run_id:    run_id,
        pattern:   "pubsub",
        backend:   backend,
        transport: "tcp",
        peers:     1,
        msg_size:  size,
        msg_count: best[:count],
        elapsed:   best[:elapsed].round(6),
        cpu_time:  best[:cpu_time].round(6),
        msgs_s:    best[:msgs_s].round(1),
        mbps:      best[:mbps].round(2),
      }
    end

    puts
  end

  FileUtils.mkdir_p(CACHE_DIR)
  File.open(JSONL_PATH, "a") do |f|
    all_rows.each { |row| f.puts(JSON.generate(row)) }
  end
  $stderr.puts "Appended #{all_rows.size} rows to #{JSONL_PATH}"

  if opts[:chart]
    system(RUBY_BIN, File.join(__dir__, "gen_pubsub_chart.rb"))
  end
end


main
