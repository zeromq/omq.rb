# frozen_string_literal: true

# PUSH/PULL throughput over ws:// and wss:// (with a tcp:// baseline for
# scale). Mirrors omq/bench/push_pull/omq.rb's prime → calibrate → measure
# shape so numbers are directly comparable.
#
# Usage:
#   OMQ_DEV=1 bundle exec ruby --yjit bench/omq.rb
#
# Env knobs (all optional):
#   OMQ_BENCH_TRANSPORTS  default "tcp,ws,wss"
#   OMQ_BENCH_SIZES       default "128,512,2048,8192,32768"
#   OMQ_BENCH_PEERS       default "1,3"
#   OMQ_BENCH_TARGET      target seconds per timed round (default 1.0)
#   OMQ_BENCH_TIMEOUT     per-cell timeout seconds (default 30)

$VERBOSE     = nil
$stdout.sync = true

require "bundler/setup"
require "omq"
require "omq/transport/websocket"
require "async"
require "console"
require "openssl"
require "json"

Console.logger = Console::Logger.new(Console::Output::Null.new)

module Bench

  SIZES           = (ENV["OMQ_BENCH_SIZES"] || "128,512,2048,8192,32768").split(",").map(&:to_i).freeze
  PEER_COUNTS     = (ENV["OMQ_BENCH_PEERS"] || "1,3").split(",").map(&:to_i).freeze
  TRANSPORTS      = (ENV["OMQ_BENCH_TRANSPORTS"] || "tcp,ws,wss").split(",").freeze
  ROUND_DURATION  = Float(ENV.fetch("OMQ_BENCH_TARGET",  1.0))
  WARMUP_DURATION = 0.3
  WARMUP_MIN_ITERS = 1_000
  PRIME_ITERS     = 5_000
  ROUNDS          = 1
  RUN_TIMEOUT     = Integer(ENV.fetch("OMQ_BENCH_TIMEOUT", 30))
  KERNEL          = `uname -r`.strip.freeze
  RESULTS_PATH    = File.join(__dir__, "results.jsonl").freeze

  module_function


  def run_id
    @run_id ||= ENV["OMQ_BENCH_RUN_ID"] || Time.now.strftime("%Y-%m-%dT%H:%M:%S")
  end


  def tls_context
    @tls_context ||= begin
      key  = OpenSSL::PKey::RSA.new(2048)
      name = OpenSSL::X509::Name.parse("/CN=127.0.0.1")
      cert = OpenSSL::X509::Certificate.new
      cert.version    = 2
      cert.serial     = 1
      cert.subject    = name
      cert.issuer     = name
      cert.public_key = key.public_key
      cert.not_before = Time.now - 60
      cert.not_after  = Time.now + 3600
      cert.sign(key, OpenSSL::Digest.new("SHA256"))

      ctx             = OpenSSL::SSL::SSLContext.new
      ctx.cert        = cert
      ctx.key         = key
      ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION
      ctx
    end
  end


  def client_tls_context
    @client_tls_context ||= begin
      ctx             = OpenSSL::SSL::SSLContext.new
      ctx.verify_mode = OpenSSL::SSL::VERIFY_NONE
      ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION
      ctx
    end
  end


  def apply_tls(socket, transport, role:)
    return unless transport == "wss"
    socket.tls_context = role == :server ? tls_context : client_tls_context
  end


  def bind_endpoint(transport)
    case transport
    when "tcp" then "tcp://127.0.0.1:0"
    when "ws"  then "ws://127.0.0.1:0"
    when "wss" then "wss://127.0.0.1:0"
    end
  end


  def connect_endpoint(transport, port)
    "#{transport}://127.0.0.1:#{port}"
  end


  def estimate_n(target: ROUND_DURATION, warmup: WARMUP_DURATION)
    n = WARMUP_MIN_ITERS
    loop do
      elapsed = Async::Clock.measure { yield n }
      if elapsed >= warmup
        rate = n / elapsed
        return [(rate * target).to_i, WARMUP_MIN_ITERS].max
      end
      n *= 4
    end
  end


  def measure_best_of(payload, align: 1, &burst)
    burst.call(PRIME_ITERS)
    n = estimate_n(&burst)
    n = [(n / align) * align, align].max

    best = nil
    ROUNDS.times do
      elapsed = Async::Clock.measure { burst.call(n) }
      best    = elapsed if best.nil? || elapsed < best
    end

    report(payload.bytesize, n, best)
  end


  def measure(receiver, senders, payload)
    burst = ->(k) {
      per     = [k / senders.size, 1].max
      barrier = Async::Barrier.new

      senders.each do |sender|
        barrier.async do
          per.times { sender << payload.dup }
        end
      end

      (per * senders.size).times { receiver.receive }

      barrier.wait
    }

    measure_best_of(payload, align: senders.size, &burst)
  end


  def report(msg_size, n, elapsed)
    mbps   = n * msg_size / elapsed / 1_000_000.0
    msgs_s = n / elapsed
    printf "  %6s  %8.1f MB/s  %8.0f msg/s  (%.2fs, n=%d)\n",
           "#{msg_size}B", mbps, msgs_s, elapsed, n
    { n: n, elapsed: elapsed, mbps: mbps, msgs_s: msgs_s }
  end


  def wait_connected(sockets)
    sockets.each { |s| s.peer_connected.wait }
  end


  def append_result(transport, peers, msg_size, msg_count, elapsed, mbps, msgs_s)
    row = {
      run_id:    run_id,
      pattern:   "push_pull",
      transport: transport,
      peers:     peers,
      msg_size:  msg_size,
      msg_count: msg_count,
      elapsed_s: elapsed.round(6),
      mbps:      mbps.round(2),
      msgs_s:    msgs_s.round(1),
    }
    File.open(RESULTS_PATH, "a") { |f| f.puts(JSON.generate(row)) }
  end


  def run_cell(transport, peers, payload)
    pull = OMQ::PULL.new
    apply_tls(pull, transport, role: :server)
    bound = pull.bind(bind_endpoint(transport))
    ep    = connect_endpoint(transport, bound.port)

    pushes = peers.times.map do
      push = OMQ::PUSH.new
      apply_tls(push, transport, role: :client)
      push.connect(ep)
      push
    end
    wait_connected(pushes)

    begin
      measure(pull, pushes, payload)
    ensure
      pushes.each(&:close)
      pull.close
    end
  end


  def run!
    jit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
    puts "PUSH/PULL | omq-websocket #{OMQ::Transport::WebSocket::VERSION} | OMQ #{OMQ::VERSION} | Ruby #{RUBY_VERSION} (#{jit}) | #{KERNEL}"
    puts

    TRANSPORTS.each do |transport|
      PEER_COUNTS.each do |peers|
        header = "#{transport} (#{peers} peer#{'s' if peers > 1})"
        puts "--- #{header} ---"
        completed = 0

        SIZES.each do |size|
          Async do |task|
            task.with_timeout(RUN_TIMEOUT) do
              r = run_cell(transport, peers, "x" * size)
              append_result(transport, peers, size, r[:n], r[:elapsed], r[:mbps], r[:msgs_s])
              completed += 1
            end
          rescue Async::TimeoutError
            abort "BENCH TIMEOUT: #{header} #{size}B exceeded #{RUN_TIMEOUT}s"
          end
        end

        abort "BENCH FAILED: #{header} produced no results" if completed == 0
        puts
      end
    end
  end

end


Bench.run!
