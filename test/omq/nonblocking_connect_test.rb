# frozen_string_literal: true

require_relative "../test_helper"

class OMQNonblockingConnectTest < Minitest::Spec
  describe "Non-blocking TCP connect" do
    it "connect returns immediately even when endpoint is unreachable" do
      Async do
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL

        elapsed = Async::Clock.measure do
          # Connect to 3 endpoints — none are listening.
          # With blocking connect, each would wait for OS timeout (~2 min).
          req.connect("tcp://127.0.0.1:19871")
          req.connect("tcp://127.0.0.1:19872")
          req.connect("tcp://127.0.0.1:19873")
        end

        # All three should return instantly (well under 1 second).
        assert_operator elapsed, :<, 0.5,
          "connect should not block (took #{elapsed}s)"
      ensure
        req&.close
      end
    end

    it "connects in background and succeeds when server appears" do
      Async do
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL
        req.connect("tcp://127.0.0.1:19874")

        # Server starts after client — connect already returned
        sleep 0.02
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.bind("tcp://127.0.0.1:19874")

        # Wait for background connect to succeed
        wait_connected(req, rep)

        req.send("async connect")
        msg = rep.receive
        assert_equal ["async connect"], msg
      ensure
        req&.close
        rep&.close
      end
    end
  end
end
