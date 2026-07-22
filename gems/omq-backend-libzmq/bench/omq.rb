# frozen_string_literal: true

# libzmq backend benchmarks: ruby vs libzmq across patterns.
#
# Usage: ruby --yjit bench/omq.rb

$VERBOSE = nil
$stdout.sync = true

require "omq"
require "omq/backend/libzmq"
require "async"
require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)

SIZES = [64, 256, 1024, 4096, 65_536].freeze
RUNS  = { 64 => 20_000, 256 => 16_000, 1024 => 12_000, 4096 => 8_000, 65_536 => 3_000 }.freeze

def report(label, msg_size, n, elapsed)
  mbps   = n * msg_size / elapsed / 1_000_000.0
  msgs_s = n / elapsed
  printf "  %-10s %6s  %8.1f MB/s  %8.0f msg/s  (%.2fs)\n",
         label, "#{msg_size}B", mbps, msgs_s, elapsed
end

def measure_throughput(backend, payload, n)
  Async do
    pull = OMQ::PULL.new(backend: backend)
    port = pull.bind("tcp://127.0.0.1:0").port

    push = OMQ::PUSH.new(backend: backend)
    push.connect("tcp://127.0.0.1:#{port}")
    sleep 0.05 if backend == :libzmq || backend == :ffi

    100.times do
      push << payload
      pull.receive
    end

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sender = Async { n.times { push << payload } }
    n.times { pull.receive }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    sender.wait

    report(backend, payload.bytesize, n, elapsed)
  ensure
    push&.close
    pull&.close
  end
end

def measure_roundtrip(backend, payload, n)
  Async do |task|
    rep = OMQ::REP.new(backend: backend)
    port = rep.bind("tcp://127.0.0.1:0").port

    req = OMQ::REQ.new(backend: backend)
    req.connect("tcp://127.0.0.1:#{port}")
    sleep 0.05 if backend == :libzmq || backend == :ffi

    responder = task.async do
      loop do
        msg = rep.receive
        rep << msg
      end
    end

    100.times do
      req << payload
      req.receive
    end

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    n.times do
      req << payload
      req.receive
    end
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

    report(backend, payload.bytesize, n, elapsed)
  ensure
    responder&.stop
    req&.close
    rep&.close
  end
end

def measure_fanout(backend, payload, n, peers:)
  Async do
    pub = OMQ::PUB.new(backend: backend)
    port = pub.bind("tcp://127.0.0.1:0").port

    subs = peers.times.map do
      sub = OMQ::SUB.new(subscribe: "", backend: backend)
      sub.connect("tcp://127.0.0.1:#{port}")
      sub
    end
    sleep 0.1

    100.times do
      pub << payload
      subs.each(&:receive)
    end

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sender = Async { n.times { pub << payload } }
    receivers = subs.map { |sub| Async { n.times { sub.receive } } }
    receivers.each(&:wait)
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    sender.wait

    report(backend, payload.bytesize, n, elapsed)
  ensure
    subs&.each(&:close)
    pub&.close
  end
end

def measure_dealer(backend, payload, n, peers:)
  Async do
    router = OMQ::ROUTER.new(backend: backend)
    port = router.bind("tcp://127.0.0.1:0").port

    dealers = peers.times.map do |i|
      d = OMQ::DEALER.new(backend: backend)
      d.identity = "d#{i}"
      d.connect("tcp://127.0.0.1:#{port}")
      d
    end
    sleep 0.05 if backend == :libzmq || backend == :ffi

    per_dealer = n / dealers.size
    100.times do
      dealers.first << payload
      router.receive
    end

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    tasks = dealers.map { |d| Async { per_dealer.times { d << payload } } }
    (per_dealer * dealers.size).times { router.receive }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    tasks.each(&:wait)

    report(backend, payload.bytesize, n, elapsed)
  ensure
    dealers&.each(&:close)
    router&.close
  end
end

def measure_interop(payload, n)
  Async do
    pull = OMQ::PULL.new
    port = pull.bind("tcp://127.0.0.1:0").port

    push = OMQ::PUSH.new(backend: :libzmq)
    push.connect("tcp://127.0.0.1:#{port}")
    sleep 0.05

    100.times do
      push << payload
      pull.receive
    end

    t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sender = Async { n.times { push << payload } }
    n.times { pull.receive }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
    sender.wait

    report("libzmq->ruby", payload.bytesize, n, elapsed)
  ensure
    push&.close
    pull&.close
  end
end

jit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
kernel = `uname -r`.strip
puts "libzmq Backend | OMQ #{OMQ::VERSION} | Ruby #{RUBY_VERSION} (#{jit}) | #{kernel}"
puts

puts "--- PUSH/PULL throughput (ruby) ---"
SIZES.each { |s| measure_throughput(:ruby, "x" * s, RUNS[s]) }
puts
puts "--- PUSH/PULL throughput (libzmq) ---"
SIZES.each { |s| measure_throughput(:libzmq, "x" * s, RUNS[s]) }
puts
puts "--- PUSH/PULL throughput (libzmq->ruby interop) ---"
SIZES.each { |s| measure_interop("x" * s, RUNS[s]) }
puts

puts "--- PUB/SUB fan-out 3 peers (ruby) ---"
SIZES.each { |s| measure_fanout(:ruby, "x" * s, RUNS[s], peers: 3) }
puts
puts "--- PUB/SUB fan-out 3 peers (libzmq) ---"
SIZES.each { |s| measure_fanout(:libzmq, "x" * s, RUNS[s], peers: 3) }
puts

puts "--- ROUTER/DEALER 3 peers (ruby) ---"
SIZES.each { |s| measure_dealer(:ruby, "x" * s, RUNS[s], peers: 3) }
puts
puts "--- ROUTER/DEALER 3 peers (libzmq) ---"
SIZES.each { |s| measure_dealer(:libzmq, "x" * s, RUNS[s], peers: 3) }
puts

puts "--- REQ/REP roundtrip (ruby) ---"
SIZES.each { |s| measure_roundtrip(:ruby, "x" * s, RUNS[s]) }
puts
puts "--- REQ/REP roundtrip (libzmq) ---"
SIZES.each { |s| measure_roundtrip(:libzmq, "x" * s, RUNS[s]) }
