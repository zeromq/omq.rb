# frozen_string_literal: true

require_relative "../test_helper"

class OMQReconnectTest < Minitest::Spec
  describe "Auto-reconnection" do
    it "connects silently when server is not yet running" do
      Async do
        # Client connects before server exists — should not raise
        req                      = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval   = RECONNECT_INTERVAL
        req.connect("tcp://127.0.0.1:19876")

        # Start server after a delay
        sleep 0.03
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.bind("tcp://127.0.0.1:19876")

        # Wait for background reconnect to succeed
        wait_connected(req, rep)

        req.send("late start")
        msg = rep.receive
        assert_equal ["late start"], msg
        rep.send("ok")
        reply = req.receive
        assert_equal ["ok"], reply
      ensure
        req&.close
        rep&.close
      end
    end

    it "reconnects after server restart over TCP" do
      Async do
        # Start server
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        # Client connects
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL
        req.connect("tcp://127.0.0.1:#{port}")

        # First exchange works
        req.send("hello")
        msg = rep.receive
        assert_equal ["hello"], msg
        rep.send("world")
        reply = req.receive
        assert_equal ["world"], reply

        # Kill the server
        rep.close

        # Wait a bit for the reconnect to kick in
        sleep 0.03

        # Restart server on same port
        rep2 = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep2.bind("tcp://127.0.0.1:#{port}")

        # Wait for reconnection
        wait_connected(req, rep2)

        # Second exchange should work after reconnection
        req.send("reconnected")
        msg = rep2.receive
        assert_equal ["reconnected"], msg
        rep2.send("yes")
        reply = req.receive
        assert_equal ["yes"], reply
      ensure
        req&.close
        rep2&.close
      end
    end
  end
end
