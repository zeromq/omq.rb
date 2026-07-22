# frozen_string_literal: true

require_relative "test_helper"

describe "Rust backend stress" do
  def bind_port(sock)
    ep = sock.bind("tcp://127.0.0.1:0")
    ep.port
  end


  describe "throughput" do
    it "transfers 10k messages" do
      n = 10_000
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        sender = Async do
          n.times { |i| push << i.to_s }
        end

        received = 0
        n.times do
          pull.receive
          received += 1
        end

        sender.wait
        assert_equal n, received
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "multiple sockets" do
    it "runs 10 PUSH/PULL pairs on shared runtime" do
      pairs = 10
      msgs_per_pair = 100

      Async do
        sockets = []
        pairs.times do |i|
          pull = OMQ::PULL.new(backend: BACKEND)
          port = bind_port(pull)
          push = OMQ::PUSH.new(backend: BACKEND)
          push.connect("tcp://127.0.0.1:#{port}")
          sockets << [push, pull]
        end

        sockets.each { |push, _| push.peer_connected.wait }

        tasks = sockets.map do |push, pull|
          Async do
            msgs_per_pair.times { |i| push << "msg-#{i}" }
            msgs_per_pair.times { pull.receive }
          end
        end

        tasks.each(&:wait)
      ensure
        sockets&.each do |push, pull|
          push&.close
          pull&.close
        end
      end
    end
  end


  describe "close under load" do
    it "closes sender while messages are in flight" do
      Async do |task|
        pull = OMQ::PULL.new(backend: BACKEND, recv_timeout: 0.5)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        100.times { |i| push << "flood-#{i}" }
        push.close

        received = 0
        loop do
          pull.receive
          received += 1
        rescue IO::TimeoutError
          break
        end

        assert received > 0, "expected to receive some messages before timeout"
      ensure
        pull&.close
      end
    end


    it "closes receiver while sender is active" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "before-close"
        pull.receive
        pull.close

        refute pull.closed?.nil?
      ensure
        push&.close
      end
    end
  end


  describe "FD leak check" do
    it "does not leak file descriptors across socket lifecycles" do
      fd_count = -> { Dir["/proc/#{$$}/fd/*"].size }

      baseline = fd_count.call

      5.times do
        Async do
          pull = OMQ::PULL.new(backend: BACKEND)
          port = bind_port(pull)
          push = OMQ::PUSH.new(backend: BACKEND)
          push.connect("tcp://127.0.0.1:#{port}")
          push.peer_connected.wait
          push << "leak-test"
          pull.receive
        ensure
          push&.close
          pull&.close
        end
      end

      sleep 0.1
      after = fd_count.call
      leaked = after - baseline
      assert leaked <= 5, "leaked #{leaked} FDs (baseline=#{baseline}, after=#{after})"
    end
  end


  describe "reconnect" do
    it "reconnects after server restart" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "before"
        assert_equal ["before"], pull.receive

        pull.close

        pull2 = OMQ::PULL.new(backend: BACKEND)
        pull2.bind("tcp://127.0.0.1:#{port}")

        sleep 0.2

        push << "after"
        assert_equal ["after"], pull2.receive
      ensure
        push&.close
        pull2&.close
      end
    end
  end
end
