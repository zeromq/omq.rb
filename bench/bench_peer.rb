#!/usr/bin/env ruby
# frozen_string_literal: true

# Standalone bench peer process, analogous to bench_peer_tokio.rs in OMQ.rs.
#
# Usage:
#   ruby bench/bench_peer.rb push <endpoint> <msg_size>
#   ruby bench/bench_peer.rb pull <endpoint> <msg_size> <duration>
#
# Environment:
#   OMQ_BENCH_BACKEND  "ruby" (default) or "rust"

$VERBOSE = nil
$stdout.sync = true

require "bundler/setup"
require "omq"
require "async"
require "console"

Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

BACKEND = (ENV.fetch("OMQ_BENCH_BACKEND", "ruby")).to_sym

case BACKEND
when :rust
  require "omq/rust"
when :ffi
  require "omq/ffi"
end

WARMUP_DURATION = 0.5


def bench_payload(size)
  "x" * size
end


def run_push(endpoint, msg_size)
  payload = bench_payload(msg_size)

  Async do |task|
    push = OMQ::PUSH.new(backend: BACKEND)
    uri  = push.bind(endpoint)

    if endpoint.include?(":0")
      $stdout.puts "PORT #{uri.port}"
      $stdout.flush
    end

    push.peer_connected.wait
    loop { push << payload }
  ensure
    push&.close
  end
end


def run_pull(endpoint, msg_size, duration)
  hard_timeout = WARMUP_DURATION + duration + 5

  Async do |task|
    task.with_timeout(hard_timeout) do
      pull = OMQ::PULL.new(backend: BACKEND)
      pull.connect(endpoint)
      pull.peer_connected.wait if BACKEND == :rust

      deadline = Async::Clock.now + WARMUP_DURATION
      while Async::Clock.now < deadline
        pull.receive
      end

      t0    = Async::Clock.now
      stop  = t0 + duration
      count = 0

      loop do
        64.times do
          pull.receive
          count += 1
        end
        break if Async::Clock.now >= stop
      end

      elapsed = Async::Clock.now - t0
      $stdout.puts "#{count} #{"%.6f" % elapsed} #{msg_size}"
    ensure
      pull&.close
    end
  rescue Async::TimeoutError
    $stderr.puts "bench_peer: pull timed out after #{hard_timeout}s"
    exit 1
  end
end


def run_pub(endpoint, msg_size)
  payload = bench_payload(msg_size)

  Async do
    pub = OMQ::PUB.new(backend: BACKEND, on_mute: :block)
    uri = pub.bind(endpoint)

    if endpoint.include?(":0")
      $stdout.puts "PORT #{uri.port}"
      $stdout.flush
    end

    case BACKEND
    when :rust, :ffi
      pub.peer_connected.wait
    else
      pub.subscriber_joined.wait
    end
    loop { pub << payload }
  ensure
    pub&.close
  end
end


def run_sub(endpoint, msg_size, duration)
  hard_timeout = WARMUP_DURATION + duration + 5

  Async do |task|
    task.with_timeout(hard_timeout) do
      sub = OMQ::SUB.new(backend: BACKEND, subscribe: "")
      sub.connect(endpoint)
      sub.peer_connected.wait if BACKEND == :rust

      deadline = Async::Clock.now + WARMUP_DURATION
      while Async::Clock.now < deadline
        sub.receive
      end

      t0    = Async::Clock.now
      stop  = t0 + duration
      count = 0

      loop do
        64.times do
          sub.receive
          count += 1
        end
        break if Async::Clock.now >= stop
      end

      elapsed = Async::Clock.now - t0
      $stdout.puts "#{count} #{"%.6f" % elapsed} #{msg_size}"
    ensure
      sub&.close
    end
  rescue Async::TimeoutError
    $stderr.puts "bench_peer: sub timed out after #{hard_timeout}s"
    exit 1
  end
end


def run_rep(endpoint, msg_size)
  Async do
    rep = OMQ::REP.new(backend: BACKEND)
    uri = rep.bind(endpoint)

    if endpoint.include?(":0")
      $stdout.puts "PORT #{uri.port}"
      $stdout.flush
    end

    loop do
      msg = rep.receive
      rep << msg.first
    end
  ensure
    rep&.close
  end
end


def run_req(endpoint, msg_size, iterations, warmup)
  payload = bench_payload(msg_size)

  Async do |task|
    task.with_timeout(iterations + 30) do
      req = OMQ::REQ.new(backend: BACKEND)
      req.connect(endpoint)
      req.peer_connected.wait if BACKEND == :rust

      warmup.times do
        req << payload
        req.receive
      end

      latencies = Array.new(iterations)

      iterations.times do |i|
        t0 = Async::Clock.now
        req << payload
        req.receive
        latencies[i] = (Async::Clock.now - t0) * 1_000_000
      end

      latencies.sort!
      p50  = latencies[(iterations * 0.50).to_i]
      p99  = latencies[(iterations * 0.99).to_i]
      p999 = latencies[(iterations * 0.999).to_i]
      max  = latencies.last

      $stdout.puts "#{"%.3f" % p50} #{"%.3f" % p99} #{"%.3f" % p999} #{"%.3f" % max} #{iterations}"
    ensure
      req&.close
    end
  rescue Async::TimeoutError
    $stderr.puts "bench_peer: req timed out"
    exit 1
  end
end


mode     = ARGV[0]
endpoint = ARGV[1]
msg_size = Integer(ARGV[2])

case mode
when "push"
  run_push(endpoint, msg_size)
when "pull"
  duration = Float(ARGV[3])
  run_pull(endpoint, msg_size, duration)
when "pub"
  run_pub(endpoint, msg_size)
when "sub"
  duration = Float(ARGV[3])
  run_sub(endpoint, msg_size, duration)
when "rep"
  run_rep(endpoint, msg_size)
when "req"
  iterations = Integer(ARGV[3])
  warmup     = Integer(ARGV[4])
  run_req(endpoint, msg_size, iterations, warmup)
else
  $stderr.puts "Unknown mode: #{mode}"
  $stderr.puts "Usage: bench_peer.rb <push|pull|pub|sub|rep|req> <endpoint> <msg_size> [args...]"
  exit 1
end
