# frozen_string_literal: true

require_relative "../test_helper"

class OMQReconnectBackoffTest < Minitest::Spec
  describe "Reconnection with Range backoff" do
    it "reconnects with exponential backoff" do
      Async do
        # Client connects to a port where nothing is listening yet
        req                    = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL..1.0

        rep  = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        # Connect, exchange, then kill the server
        req.connect("tcp://127.0.0.1:#{port}")
        req.send("hello")
        assert_equal ["hello"], rep.receive
        rep.send("world")
        assert_equal ["world"], req.receive

        rep.close

        # Wait for the connection to drop
        sleep 0.02

        # Restart server on same port
        rep2 = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep2.bind("tcp://127.0.0.1:#{port}")

        # Wait for backoff reconnection
        wait_connected(req, rep2)

        # Should reconnect and work
        req.send("reconnected")
        msg = rep2.receive
        assert_equal ["reconnected"], msg
      ensure
        req&.close
        rep2&.close
      end
    end

    it "uses fixed interval when reconnect_interval is a number" do
      Async do
        req                    = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL

        rep  = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        req.connect("tcp://127.0.0.1:#{port}")
        req.send("first")
        assert_equal ["first"], rep.receive
        rep.send("ok")
        req.receive

        rep.close
        sleep 0.02

        rep2 = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(req, rep2)

        req.send("second")
        assert_equal ["second"], rep2.receive
      ensure
        req&.close
        rep2&.close
      end
    end
  end
end
