# frozen_string_literal: true

require_relative "../test_helper"

describe "Edge cases" do
  before { OMQ::Transport::Inproc.reset! }

  describe "large identity" do
    it "DEALER with 255-byte identity connects to ROUTER" do
      Async do
        router = OMQ::ROUTER.bind("ruby://edge-bigid")
        dealer = OMQ::DEALER.new
        dealer.identity = "x" * 255
        dealer.connect("ruby://edge-bigid")

        dealer.send("hello")
        msg = router.receive
        assert_equal "x" * 255, msg[0]
        assert_equal "hello", msg[1]
      ensure
        dealer&.close
        router&.close
      end
    end
  end

  describe "rapid connect/disconnect cycles" do
    it "survives 20 rapid cycles over inproc" do
      Async do
        pull = OMQ::PULL.bind("ruby://edge-rapid")

        20.times do |i|
          push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
          push.connect("ruby://edge-rapid")
          push.send("msg-#{i}")
          Async::Task.current.yield
          push.close
        end

        received = 0
        pull.recv_timeout = 0.05
        loop do
          pull.receive
          received += 1
        rescue IO::TimeoutError
          break
        end

        assert_operator received, :>, 0, "expected at least some messages"
      ensure
        pull&.close
      end
    end

    it "survives 10 rapid cycles over TCP" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://127.0.0.1:0").port

        10.times do |i|
          push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
          push.connect("tcp://127.0.0.1:#{port}")
          sleep 0.01
          push.send("msg-#{i}")
          push.close
        end

        received = 0
        pull.recv_timeout = 0.05
        loop do
          pull.receive
          received += 1
        rescue IO::TimeoutError
          break
        end

        assert_operator received, :>, 0, "expected at least some messages"
      ensure
        pull&.close
      end
    end
  end

  describe "bind to already-bound address" do
    it "raises on duplicate TCP bind" do
      Async do
        rep1 = OMQ::REP.new
        port = rep1.bind("tcp://127.0.0.1:0").port

        assert_raises(Errno::EADDRINUSE) do
          OMQ::REP.bind("tcp://127.0.0.1:#{port}")
        end
      ensure
        rep1&.close
      end
    end

    it "raises on duplicate inproc bind" do
      Async do
        rep1 = OMQ::REP.bind("ruby://edge-dupbind")

        assert_raises(RuntimeError, ArgumentError) do
          OMQ::REP.bind("ruby://edge-dupbind")
        end
      ensure
        rep1&.close
      end
    end
  end
end
