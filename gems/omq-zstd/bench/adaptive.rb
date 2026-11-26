# frozen_string_literal: true

# Adaptive level vs a fixed one: PUSH -> PULL over zstd+tcp:// through a
# proxy process that forwards the sender's bytes at a set rate (none, or a
# slow link), to a receiver in a third process. Reports plaintext MB/s and wire ratio after the level settled,
# and the level the connection ended at.
#
# Run:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/adaptive.rb
#   CORPUS=path/to/file ...   # messages are slices of the file instead

$stdout.sync = true

require "socket"
require "async"
require "omq"
require "omq/zstd"

SECONDS = 6.0   # per case; the last half is measured
SETTLE  = 3.0
SIZE    = 4096
CORPUS  = ENV["CORPUS"].to_s[/.+/]
RATES   = { "unlimited" => nil, "10 MB/s" => 10_000_000, "2 MB/s" => 2_000_000 }.freeze
LEVELS  = { "level: -3" => -3, "adaptive" => nil }.freeze


# JSON log records with varying fields: compressible, not degenerate.
def message(rng)
  out = +""
  while out.bytesize < SIZE
    out << %({"ts":"2026-11-26T12:#{format("%02d", rng.rand(60))}:#{format("%02d", rng.rand(60))}Z","level":"#{%w[INFO WARN DEBUG][rng.rand(3)]}","user":#{rng.rand(10_000)},"path":"/v1/items/#{rng.rand(500)}","ms":#{rng.rand(900)}}\n)
  end
  out.byteslice(0, SIZE).b
end


# Forwards client bytes to `target_port` at `rate` bytes/s (nil: as fast as
# it can) and counts them. Returns [pid, port, count_reader].
def start_proxy(target_port, rate)
  server        = TCPServer.new("127.0.0.1", 0)
  port          = server.addr[1]
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    client   = server.accept
    upstream = TCPSocket.new("127.0.0.1", target_port)
    Thread.new { IO.copy_stream(upstream, client) rescue nil }
    total   = 0
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    trap("TERM") { writer.puts(total); exit!(0) }
    loop do
      chunk = client.readpartial(16 * 1024)
      upstream.write(chunk)
      total += chunk.bytesize
      next unless rate

      ahead = total.to_f / rate - (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
      sleep(ahead) if ahead > 0
    end
  rescue EOFError, IOError, SystemCallError
    writer.puts(total)
  end
  server.close
  writer.close
  [pid, port, reader]
end


# Runs PULL in its own process, so the sender's reactor only compresses.
# Returns [pid, port, reader]; the reader yields "received measured".
def start_receiver
  reader, writer = IO.pipe
  pid = fork do
    reader.close
    Sync do |task|
      pull = OMQ::PULL.new
      writer.puts(pull.bind("zstd+tcp://127.0.0.1:0").port)
      received = 0
      measured = 0
      start    = nil
      trap("TERM") { writer.puts("#{received} #{measured}"); exit!(0) }
      loop do
        parts = pull.receive
        start ||= Async::Clock.now
        received += parts.first.bytesize
        measured += parts.first.bytesize if Async::Clock.now - start >= SETTLE
      end
    end
  end
  writer.close
  [pid, reader.gets.to_i, reader]
end


def run_case(rate, level)
  rng      = Random.new(1)
  corpus   = CORPUS && File.binread(CORPUS, 64 * SIZE * 16)
  messages = Array.new(64) { |i| [corpus ? corpus.byteslice(i * SIZE * 16, SIZE) : message(rng)] }

  receiver, target, results = start_receiver
  proxy, port, counts       = start_proxy(target, rate)
  final = nil

  Sync do
    push = OMQ::PUSH.new(linger: 0)
    level ? push.connect("zstd+tcp://127.0.0.1:#{port}", level: level) : push.connect("zstd+tcp://127.0.0.1:#{port}")

    start = Async::Clock.now
    i     = 0
    while Async::Clock.now - start < SECONDS
      push << messages[i % messages.size]
      i += 1
    end

    sender = push.instance_variable_get(:@engine).connections.keys.first.instance_variable_get(:@sender)
    final  = sender ? sender.frame_codec.level : level
    push.close
  end

  [receiver, proxy].each { |pid| Process.kill("TERM", pid) }
  received, measured = results.gets.split.map(&:to_i)
  wire = counts.read.to_i
  [receiver, proxy].each { |pid| Process.wait(pid) }
  { mbps: measured / (SECONDS - SETTLE) / 1e6, ratio: wire.to_f / [received, 1].max, level: final }
end


puts "omq-zstd v#{OMQ::Zstd::VERSION}, zrip v#{Zrip::VERSION}, #{SIZE} B #{CORPUS ? File.basename(CORPUS) : "JSON"} messages, YJIT #{RubyVM::YJIT.enabled? ? "on" : "off"}"
RATES.each do |rate_name, rate|
  LEVELS.each do |level_name, level|
    r = run_case(rate, level)
    puts format("  %-10s %-10s  %7.1f MB/s plaintext  wire ratio %.3f  level %d", rate_name, level_name, r[:mbps], r[:ratio], r[:level])
  end
end
