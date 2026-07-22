#!/usr/bin/env ruby
# frozen_string_literal: true

# REQ/REP 2-process latency benchmark with backend comparison.
#
# Spawns separate rep (server) and req (client) processes per cell,
# measures roundtrip latency percentiles, writes JSONL to ~/.cache/omq/ruby/.
#
# Usage:
#   ruby --yjit bench/bench_reqrep.rb                         # auto-detect backends
#   ruby --yjit bench/bench_reqrep.rb --backends ruby,rust    # specific backends
#   ruby --yjit bench/bench_reqrep.rb --quick                 # 3 sizes, fewer iterations
#   ruby --yjit bench/bench_reqrep.rb --chart                 # generate SVG after

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

DEFAULT_ITERATIONS = 50_000
DEFAULT_WARMUP     = 5_000
DEFAULT_ROUNDS     = 3
QUICK_ITERATIONS   = 10_000
QUICK_WARMUP       = 1_000
QUICK_ROUNDS       = 1

CELL_TIMEOUT = 60

CACHE_DIR  = File.join(ENV.fetch("XDG_CACHE_HOME", File.join(Dir.home, ".cache")), "omq", "ruby")
JSONL_PATH = File.join(CACHE_DIR, "results_reqrep.jsonl")


def parse_args
  opts = {
    backends:   nil,
    sizes:      CHART_SIZES,
    iterations: DEFAULT_ITERATIONS,
    warmup:     DEFAULT_WARMUP,
    rounds:     DEFAULT_ROUNDS,
    chart:      false,
  }

  args = ARGV.dup
  while (arg = args.shift)
    case arg
    when "--backends"
      opts[:backends] = args.shift.split(",")
    when "--sizes"
      opts[:sizes] = args.shift.split(",").map(&:to_i)
    when "--iterations"
      opts[:iterations] = Integer(args.shift)
    when "--warmup"
      opts[:warmup] = Integer(args.shift)
    when "--rounds"
      opts[:rounds] = Integer(args.shift)
    when "--quick"
      opts[:sizes]      = QUICK_SIZES
      opts[:iterations] = QUICK_ITERATIONS
      opts[:warmup]     = QUICK_WARMUP
      opts[:rounds]     = QUICK_ROUNDS
    when "--chart"
      opts[:chart] = true
    end
  end

  opts[:backends] ||= detect_backends
  opts
end


def detect_backends
  available = ["ruby"]

  { "rust" => "omq/rust", "libzmq" => "omq/backend/libzmq" }.each do |backend, lib|
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


def run_cell(backend, msg_size, iterations, warmup)
  rep_pid = nil
  req_pid = nil
  rep_out = nil
  req_out = nil

  Timeout.timeout(CELL_TIMEOUT) do
    rep_pid, rep_out = spawn_peer("rep", "tcp://127.0.0.1:0", msg_size, backend: backend)

    line = rep_out.gets
    port = line && line.strip.match(/^PORT (\d+)$/)&.[](1)&.to_i
    return nil unless port

    sleep 0.15

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    req_pid, req_out = spawn_peer("req", "tcp://127.0.0.1:#{port}", msg_size,
                                  iterations, warmup, backend: backend)
    output = req_out.read.strip
    req_out.close
    req_out = nil
    Process.wait(req_pid)
    req_pid = nil

    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    rep_cpu = ChartHelper.read_proc_cpu(rep_pid)

    rep_out.close
    rep_out = nil
    kill_peer(rep_pid)
    rep_pid = nil

    return nil if output.empty?

    parts = output.split
    return nil if parts.size < 5

    {
      p50:        Float(parts[0]),
      p99:        Float(parts[1]),
      p999:       Float(parts[2]),
      max:        Float(parts[3]),
      iterations: Integer(parts[4]),
      elapsed:    elapsed,
      cpu_time:   rep_cpu,
    }
  end
rescue Timeout::Error
  nil
ensure
  rep_out&.close
  req_out&.close
  kill_peer(rep_pid) if rep_pid
  kill_peer(req_pid) if req_pid
end


def main
  opts   = parse_args
  run_id = "ts-#{Time.now.to_i}"

  jit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
  puts "REQ/REP | 2-process TCP loopback | Ruby #{RUBY_VERSION} (#{jit})"
  puts "run_id: #{run_id}"
  puts "iterations: #{opts[:iterations]}, warmup: #{opts[:warmup]}"
  puts

  all_rows = []

  opts[:backends].each do |backend|
    puts "--- #{backend} backend ---"

    opts[:sizes].each do |size|
      best = nil

      opts[:rounds].times do
        cell = run_cell(backend, size, opts[:iterations], opts[:warmup])
        if cell && (best.nil? || cell[:p50] < best[:p50])
          best = cell
        end
      end

      if best.nil?
        printf "  ~%6sB  FAILED\n", size
        next
      end

      cpu_pct = best[:elapsed] > 0 ? best[:cpu_time] / best[:elapsed] * 100 : 0

      printf "  ~%6sB  p50 %7.1f µs  p99 %7.1f µs  p999 %8.1f µs  max %8.1f µs  cpu %5.1f%%\n",
             size, best[:p50], best[:p99], best[:p999], best[:max], cpu_pct

      all_rows << {
        run_id:     run_id,
        pattern:    "reqrep",
        backend:    backend,
        transport:  "tcp",
        msg_size:   size,
        iterations: best[:iterations],
        p50:        best[:p50].round(3),
        p99:        best[:p99].round(3),
        p999:       best[:p999].round(3),
        max:        best[:max].round(3),
        elapsed:    best[:elapsed].round(6),
        cpu_time:   best[:cpu_time].round(6),
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
    system(RUBY_BIN, File.join(__dir__, "gen_reqrep_chart.rb"))
  end
end


main
