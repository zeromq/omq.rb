# frozen_string_literal: true

require_relative "../test_helper"

class OMQHeartbeatTest < Minitest::Spec
  describe "Heartbeat" do
    it "sends PING and receives PONG over TCP" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.heartbeat_interval = 0.05
        rep.heartbeat_ttl      = 0.5
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.heartbeat_interval = 0.05
        req.heartbeat_ttl      = 0.5
        req.connect("tcp://127.0.0.1:#{port}")

        # Exchange a message (heartbeats run in background)
        req.send("hello")
        msg = rep.receive
        assert_equal ["hello"], msg

        rep.send("world")
        reply = req.receive
        assert_equal ["world"], reply

        # Wait — heartbeats should keep the connection alive
        sleep 0.02

        # Another exchange should still work
        req.send("still alive")
        msg = rep.receive
        assert_equal ["still alive"], msg

        rep.send("yes")
        reply = req.receive
        assert_equal ["yes"], reply
      ensure
        req&.close
        rep&.close
      end
    end

    it "treats a zero heartbeat_interval as disabled" do
      parent = Async::Barrier.new
      opts   = OMQ::Options.new
      opts.heartbeat_interval = 0

      conn = Struct.new(:on_peer_ttl) do
        def touch_heartbeat
        end
      end.new

      assert_nil OMQ::Engine::Heartbeat.start(parent, conn, opts)
      assert_empty parent.tasks
      assert conn.on_peer_ttl, "watches the peer's TTL once it sends one"
    end


    it "advertises no TTL unless heartbeat_ttl is set" do
      opts = OMQ::Options.new
      opts.heartbeat_interval = 0.1
      assert_equal 0, OMQ::Engine::Heartbeat.advertised_ttl(opts)

      opts.heartbeat_ttl = 0.5
      assert_equal 0.5, OMQ::Engine::Heartbeat.advertised_ttl(opts)
    end


    it "drops a peer that goes silent past the TTL in its PING" do
      Async do
        pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
        port = pull.bind("tcp://127.0.0.1:0").port

        raw = raw_zmtp_peer(port, "PUSH")
        raw.send_command(Protocol::ZMTP::Codec::Command.ping(ttl: 0.1)) # deciseconds on the wire
        pull.peer_connected.wait

        # No local heartbeat: only the peer's TTL can end this.
        pull.all_peers_gone.wait
        pass
      ensure
        raw&.close
        pull&.close
      end
    end


    it "keeps a connection with zero heartbeat_interval up" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.heartbeat_interval = 0
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.heartbeat_interval = 0
        events = []
        req.monitor { |event| events << event.type }
        req.connect("tcp://127.0.0.1:#{port}")

        5.times do |i|
          req.send("ping #{i}")
          assert_equal ["ping #{i}"], rep.receive
          rep.send("pong #{i}")
          assert_equal ["pong #{i}"], req.receive
        end

        assert_equal 1, events.count(:handshake_succeeded)
        refute_includes events, :disconnected
      ensure
        req&.close
        rep&.close
      end
    end


    it "treats a zero heartbeat_timeout as the heartbeat_interval" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.heartbeat_interval = 0.01
        rep.heartbeat_timeout  = 0
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.heartbeat_interval = 0.01
        req.heartbeat_timeout  = 0
        events = []
        req.monitor { |event| events << event.type }
        req.connect("tcp://127.0.0.1:#{port}")
        wait_connected(req, rep)

        # Several heartbeat periods pass. PONGs keep the link alive.
        sleep 0.05

        req.send("still up")
        assert_equal ["still up"], rep.receive
        refute_includes events, :disconnected
      ensure
        req&.close
        rep&.close
      end
    end


    it "keeps an idle connection up when heartbeat_timeout equals the interval" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.heartbeat_interval = 0.01
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.heartbeat_interval = 0.01
        events = []
        req.monitor { |event| events << event.type }
        req.connect("tcp://127.0.0.1:#{port}")
        wait_connected(req, rep)

        # Only PING/PONG traffic for many heartbeat periods.
        sleep 0.15

        refute_includes events, :disconnected
      ensure
        req&.close
        rep&.close
      end
    end


    it "expires a silent peer while a send is blocked on it" do
      Async do |task|
        server = TCPServer.new("127.0.0.1", 0)
        port   = server.addr[1]

        # Handshakes, then never reads or writes again.
        peer = task.async do
          sock = server.accept
          conn = Protocol::ZMTP::Connection.new(IO::Stream::Buffered.wrap(sock), socket_type: "PULL", as_server: true)
          conn.handshake!
          sleep
        ensure
          sock&.close
        end

        push = OMQ::PUSH.new(linger: 0)
        push.heartbeat_interval = 0.02
        push.heartbeat_timeout  = 0.06
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        flood = task.async do
          loop { push << "x" * 65_536 }
        end

        push.all_peers_gone.wait
        pass
      ensure
        flood&.stop
        peer&.stop
        push&.close
        server&.close
      end
    end


    it "keeps a live peer while the application doesn't read" do
      Async do |task|
        pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
        pull.recv_hwm           = 1
        pull.heartbeat_interval = 0.01
        events = []
        pull.monitor { |event| events << event.type }
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
        push.connect("tcp://127.0.0.1:#{port}")
        flood = task.async do
          loop { push << "x" * 1024 }
        end

        # Many heartbeat periods with the receive queue full.
        sleep 0.15

        refute_includes events, :disconnected
        assert_equal ["x" * 1024], pull.receive
      ensure
        flood&.stop
        push&.close
        pull&.close
      end
    end


    it "detects dead peer via heartbeat timeout" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.heartbeat_interval = 0.02
        rep.heartbeat_timeout  = 0.06
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        # Exchange one message
        req.send("ping")
        msg = rep.receive
        assert_equal ["ping"], msg
        rep.send("pong")
        req.receive

        # Close the REQ side abruptly (simulating dead peer)
        req.close

        # Wait for heartbeat timeout to detect the dead peer
        sleep 0.08

        # REP should have detected dead peer. Try to receive — should timeout.
        rep.recv_timeout = 0.02
        assert_raises(IO::TimeoutError) { rep.receive }
      ensure
        rep&.close
      end
    end
  end
end
