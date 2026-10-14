# frozen_string_literal: true

require_relative "../test_helper"

# Disconnect-then-reconnect must work for ALL socket types.
# The connecting side detects peer loss and auto-reconnects.
#
class OMQReconnectAllTypesTest < Minitest::Spec
  describe "Reconnect after server restart" do

    # Helper: connect side reconnects after bind side restarts on same port.
    #
    def assert_reconnects(bind_class, connect_class, port: nil, &exchange)
      Async do
        server = bind_class.new
        server.linger = 0
        port = server.bind("tcp://127.0.0.1:0").port

        client = connect_class.new
        client.linger = 0
        client.reconnect_interval = RECONNECT_INTERVAL
        client.connect("tcp://127.0.0.1:#{port}")
        wait_connected(client, server)

        # First exchange
        exchange.call(client, server)

        # Kill server
        server.close
        sleep 0.02

        # Restart on same port
        server2 = bind_class.new
        server2.linger = 0
        server2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(client, server2)

        # Second exchange after reconnect
        exchange.call(client, server2)
      ensure
        client&.close
        server2&.close
      end
    end

    # ── Classic socket types ──────────────────────────────────────────

    it "REQ/REP" do
      assert_reconnects(OMQ::REP, OMQ::REQ) do |req, rep|
        req.send("ping")
        assert_equal ["ping"], rep.receive
        rep.send("pong")
        assert_equal ["pong"], req.receive
      end
    end

    it "PUSH/PULL" do
      assert_reconnects(OMQ::PULL, OMQ::PUSH) do |push, pull|
        push.send("data")
        assert_equal ["data"], pull.receive
      end
    end

    it "DEALER/REP" do
      assert_reconnects(OMQ::REP, OMQ::DEALER) do |dealer, rep|
        dealer.send(["", "request"])
        assert_equal ["request"], rep.receive
        rep.send("reply")
        msg = dealer.receive
        assert_includes msg, "reply"
      end
    end

    it "PUB/SUB" do
      Async do
        pub = OMQ::PUB.new.tap { |s| s.linger = 0 }
        port = pub.bind("tcp://127.0.0.1:0").port

        sub = OMQ::SUB.new.tap { |s| s.linger = 0 }
        sub.reconnect_interval = RECONNECT_INTERVAL
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.subscribe("")
        pub.subscriber_joined.wait

        pub.send("first")
        assert_equal ["first"], sub.receive

        pub.close
        sleep 0.02

        pub2 = OMQ::PUB.new.tap { |s| s.linger = 0 }
        pub2.bind("tcp://127.0.0.1:#{port}")
        pub2.subscriber_joined.wait

        pub2.send("second")
        assert_equal ["second"], sub.receive
      ensure
        sub&.close
        pub2&.close
      end
    end

    it "PAIR" do
      assert_reconnects(OMQ::PAIR, OMQ::PAIR) do |client, server|
        client.send("hello")
        assert_equal ["hello"], server.receive
      end
    end
  end
end
