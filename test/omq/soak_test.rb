# frozen_string_literal: true

# Soak tests: long-running stress scenarios ported from omq.rs soak suite.
#
# These run for SOAK_DURATION seconds (default 10, set via env var).
# Skip with: OMQ_SKIP_SOAK=1 rake test
#
#   OMQ_DEV=1 bundle exec ruby --yjit test/omq/soak_test.rb
#   SOAK_DURATION=60 OMQ_DEV=1 bundle exec ruby --yjit test/omq/soak_test.rb

require_relative "../test_helper"

SOAK_DURATION = Float(ENV.fetch("SOAK_DURATION", "10"))
SOAK_TIMEOUT  = SOAK_DURATION + 30

module Kernel
  remove_method :Async

  def Async(&block)
    return _Async_base unless block
    return _Async_base(&block) unless Thread.current == Thread.main

    _Async_base do |task|
      task.with_timeout(SOAK_TIMEOUT) { block.call(task) }
    end
  end
end


def drain(socket)
  socket.read_timeout = 0.001
  count = 0
  loop do
    socket.receive
    count += 1
  rescue IO::TimeoutError
    break
  end
  count
end


describe "Soak tests" do
  before do
    skip "set SOAK=1 to run soak tests" unless ENV["SOAK"] == "1"
    OMQ::Transport::Inproc.reset!
  end


  it "peer churn: PUSH with PULL peers joining and leaving" do
    Async do
      push = OMQ::PUSH.new(send_hwm: 1024)
      port = push.bind("tcp://127.0.0.1:0").port

      initial_pull = OMQ::PULL.new(recv_hwm: 64)
      initial_pull.connect("tcp://127.0.0.1:#{port}")
      push.peer_connected.wait

      peers = [initial_pull]
      sent  = 0
      start = Async::Clock.now

      while Async::Clock.now - start < SOAK_DURATION
        action = rand(10)

        if action < 3 && peers.size < 20
          pull = OMQ::PULL.new(recv_hwm: 64)
          pull.connect("tcp://127.0.0.1:#{port}")
          peers << pull
        elsif action < 5 && peers.size > 1
          idx  = rand(peers.size)
          peer = peers.delete_at(idx)
          peer.close
        end

        100.times do
          push << "soak"
          sent += 1
        end

        peers.each { |peer| drain(peer) }
        Async::Task.current.yield
      end

      peers.each(&:close)
      push.close

      elapsed = Async::Clock.now - start
      $stderr.puts "[peer_churn] done: #{sent} messages in %.1fs" % elapsed
      assert sent > 0, "no messages sent"
    end
  end


  it "reconnect storm: repeated bind/close cycles" do
    Async do
      probe = OMQ::PULL.new
      port  = probe.bind("tcp://127.0.0.1:0").port
      probe.close

      push = OMQ::PUSH.new(send_hwm: 16)
      push.linger            = 0
      push.reconnect_interval = 0.01
      push.connect("tcp://127.0.0.1:#{port}")

      start     = Async::Clock.now
      cycles    = 0
      delivered = 0

      while Async::Clock.now - start < SOAK_DURATION
        pull = OMQ::PULL.new
        pull.linger = 0

        bound = false
        40.times do
          begin
            pull.bind("tcp://127.0.0.1:#{port}")
            bound = true
            break
          rescue Errno::EADDRINUSE
            sleep 0.025
          end
        end

        unless bound
          pull.close
          next
        end

        tag = "c-#{cycles}"
        push << tag

        pull.read_timeout = 5
        begin
          msg = pull.receive
          delivered += 1 if msg.first == tag
        rescue IO::TimeoutError
          # miss
        end

        pull.close
        cycles += 1
      end

      push.close

      pct = cycles > 0 ? delivered.to_f / cycles * 100 : 100
      elapsed = Async::Clock.now - start
      $stderr.puts "[reconnect_storm] done: %d/%d delivered (%.1f%%) in %.1fs" % [delivered, cycles, pct, elapsed]
      assert cycles > 0, "no cycles completed"
      assert pct >= 50, "reconnect storm delivery rate too low: %.1f%%" % pct
    end
  end


  it "pub/sub churn: subscribers joining and leaving with different topics" do
    topics = ["fast.", "slow.", "all.", "rare."]

    Async do
      pub = OMQ::PUB.new
      port = pub.bind("tcp://127.0.0.1:0").port

      initial_sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "")
      pub.subscriber_joined.wait

      subs       = [initial_sub]
      pub_count  = 0
      start      = Async::Clock.now
      last_churn = start

      while Async::Clock.now - start < SOAK_DURATION
        1000.times do
          topic = topics[pub_count % topics.size]
          pub << "#{topic}#{pub_count}"
          pub_count += 1
        end

        subs.each { |sub| drain(sub) }

        now = Async::Clock.now
        if now - last_churn >= 0.5
          last_churn = now

          if subs.size > 1 && rand < 0.5
            idx = rand(subs.size)
            sub = subs.delete_at(idx)
            sub.close
          end

          if subs.size < 10
            prefix = topics.sample
            sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: prefix)
            subs << sub
          end
        end

        Async::Task.current.yield
      end

      subs.each(&:close)
      pub.close

      elapsed = Async::Clock.now - start
      $stderr.puts "[pub_sub_churn] done: %d published in %.1fs" % [pub_count, elapsed]
      assert pub_count > 0, "no messages published"
    end
  end


  it "large message throughput: sustained 1 MiB messages" do
    msg_size = 1024 * 1024

    Async do |task|
      pull = OMQ::PULL.new(recv_hwm: 4)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(send_hwm: 4)
      push.connect("tcp://127.0.0.1:#{port}")
      push.peer_connected.wait

      payload = "x" * msg_size
      sent    = 0
      recvd   = 0
      stop    = false

      sender = task.async do
        until stop
          push << payload
          sent += 1
        end
      end

      receiver = task.async do
        until stop
          msg = pull.receive
          assert_equal msg_size, msg.first.bytesize
          recvd += 1
        end
      end

      sleep SOAK_DURATION
      stop = true

      sender.wait
      recvd += drain(pull)
      receiver.stop

      mibs = recvd.to_f * msg_size / SOAK_DURATION / 1_048_576
      $stderr.puts "[large_throughput] done: sent %d, recvd %d in %.1fs (%.1f MiB/s)" % [sent, recvd, SOAK_DURATION, mibs]

      assert recvd > 0, "no messages received"
    ensure
      push&.close
      pull&.close
    end
  end


  it "multi socket: many pairs of different types running concurrently" do
    Async do
      pairs = []

      5.times do
        pull = OMQ::PULL.new(recv_hwm: 16)
        port = pull.bind("tcp://127.0.0.1:0").port
        push = OMQ::PUSH.new(send_hwm: 16)
        push.connect("tcp://127.0.0.1:#{port}")
        pairs << { sender: push, receiver: pull, kind: "push/pull" }
      end

      5.times do |i|
        pub = OMQ::PUB.bind("ruby://soak-multi-ps-#{i}")
        sub = OMQ::SUB.connect("ruby://soak-multi-ps-#{i}", subscribe: "")
        pairs << { sender: pub, receiver: sub, kind: "pub/sub" }
      end

      3.times do
        rep = OMQ::REP.new
        port = rep.bind("tcp://127.0.0.1:0").port
        req = OMQ::REQ.connect("tcp://127.0.0.1:#{port}")
        pairs << { sender: req, receiver: rep, kind: "req/rep" }
      end

      pairs.each do |p|
        case p[:kind]
        when "push/pull", "req/rep"
          p[:sender].peer_connected.wait
        when "pub/sub"
          p[:sender].subscriber_joined.wait
        end
      end

      start           = Async::Clock.now
      total_exchanged = 0

      while Async::Clock.now - start < SOAK_DURATION
        10.times do
          pairs.each do |pair|
            pair[:sender] << "multi"

            if pair[:kind] == "req/rep"
              msg = pair[:receiver].receive
              pair[:receiver] << msg.first
              pair[:sender].receive
              total_exchanged += 1
            end
          end
        end

        pairs.each do |pair|
          next if pair[:kind] == "req/rep"
          total_exchanged += drain(pair[:receiver])
        end

        Async::Task.current.yield
      end

      pairs.each do |p|
        p[:sender].close
        p[:receiver].close
      end

      elapsed = Async::Clock.now - start
      $stderr.puts "[multi_socket] done: %d messages across %d pairs in %.1fs" % [total_exchanged, pairs.size, elapsed]
      assert total_exchanged > 0, "no messages exchanged"
    end
  end
end
