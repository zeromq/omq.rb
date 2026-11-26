# frozen_string_literal: true

require_relative "../test_helper"

class OMQDisconnectTest < Minitest::Spec
  describe "disconnect / unbind" do
    before { OMQ::Transport::Inproc.reset! }

    # Long enough for several reconnect attempts at RECONNECT_INTERVAL.
    RETRY_WINDOW = RECONNECT_INTERVAL * 5


    {
      "tcp"    => -> { "tcp://127.0.0.1:#{TCPServer.open("127.0.0.1", 0) { it.addr[1] }}" },
      "ipc"    => -> { "ipc://@omq-test-disconnect-#{$$}-#{rand(1 << 30)}" },
      "inproc" => -> { "inproc://disconnect-retry-#{rand(1 << 30)}" },
    }.each do |name, make_endpoint|
      it "#disconnect stops a pending connect (#{name})" do
        Async do
          endpoint = make_endpoint.call

          push = OMQ::PUSH.new(linger: 0)
          push.reconnect_interval = RECONNECT_INTERVAL
          push.connect(endpoint)
          push.disconnect(endpoint)

          pull = OMQ::PULL.bind(endpoint)
          sleep RETRY_WINDOW

          assert_equal 0, push.connection_count
          assert_equal 0, pull.connection_count
        ensure
          push&.close
          pull&.close
        end
      end


      it "#disconnect stops a reconnect after the peer left (#{name})" do
        Async do
          endpoint = make_endpoint.call
          pull     = OMQ::PULL.bind(endpoint)

          push = OMQ::PUSH.new(linger: 0)
          push.reconnect_interval = RECONNECT_INTERVAL
          push.connect(endpoint)
          push.peer_connected.wait

          pull.close
          push.all_peers_gone.wait
          push.disconnect(endpoint)

          pull = OMQ::PULL.bind(endpoint)
          sleep RETRY_WINDOW

          assert_equal 0, push.connection_count
          assert_equal 0, pull.connection_count
        ensure
          push&.close
          pull&.close
        end
      end


      it "connect, #disconnect, connect leaves one connection (#{name})" do
        Async do
          endpoint = make_endpoint.call

          push = OMQ::PUSH.new(linger: 0)
          push.reconnect_interval = RECONNECT_INTERVAL
          push.connect(endpoint)
          push.disconnect(endpoint)
          push.connect(endpoint)

          pull = OMQ::PULL.bind(endpoint)
          push.peer_connected.wait
          sleep RETRY_WINDOW

          assert_equal 1, push.connection_count
          assert_equal 1, pull.connection_count
        ensure
          push&.close
          pull&.close
        end
      end
    end


    it "#disconnect closes only connections to that endpoint" do
      Async do
        pull1 = OMQ::PULL.bind("ruby://ep1")
        pull1.read_timeout = 0.05
        pull2 = OMQ::PULL.bind("ruby://ep2")
        pull2.read_timeout = 0.05

        push = OMQ::PUSH.new
        push.connect("ruby://ep1")
        push.connect("ruby://ep2")

        # Disconnect from ep1 — ep2 keeps working. PUSH uses work-stealing
        # rather than strict round-robin, so we can't assert which peer a
        # pre-disconnect message lands on; we only verify that after the
        # disconnect, the surviving endpoint still receives.
        push.disconnect("ruby://ep1")

        10.times { |i| push.send("post-#{i}") }
        received = []
        loop do
          received << pull2.receive.first
        rescue IO::TimeoutError
          break
        end
        assert_equal 10, received.size

        # ep1 must receive nothing new
        assert_raises(IO::TimeoutError) { pull1.receive }
      ensure
        push&.close
        pull1&.close
        pull2&.close
      end
    end

    it "#disconnect emits :disconnected on the monitor queue" do
      Async do
        events = []
        pull   = OMQ::PULL.bind("ruby://ep-mon")
        push   = OMQ::PUSH.new
        push.monitor { |e| events << e.type }
        push.connect("ruby://ep-mon")
        wait_connected(push)

        push.disconnect("ruby://ep-mon")
        sleep 0.01 # let monitor drain

        assert_includes events, :disconnected
      ensure
        push&.close
        pull&.close
      end
    end


    it "#unbind stops accepting new connections" do
      Async do
        rep = OMQ::REP.new
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new
        req.connect("tcp://127.0.0.1:#{port}")

        # Works before unbind
        req.send("hello")
        msg = rep.receive
        assert_equal ["hello"], msg
        rep.send("world")
        req.receive

        # Unbind
        rep.unbind("tcp://127.0.0.1:#{port}")

        # New connections silently retry in background (no raise)
        req2 = OMQ::REQ.new
        req2.connect("tcp://127.0.0.1:#{port}")  # does not raise
      ensure
        req&.close
        req2&.close
        rep&.close
      end
    end
  end
end
