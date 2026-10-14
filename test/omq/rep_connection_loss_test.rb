# frozen_string_literal: true

require_relative "../test_helper"

class OMQRepConnectionLossTest < Minitest::Spec
  describe "REP connection loss with pending reply" do
    it "discards pending reply when connection drops" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        req1 = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req1.connect("tcp://127.0.0.1:#{port}")

        # First client sends a request
        req1.send("from-req1")
        msg = rep.receive
        assert_equal ["from-req1"], msg

        # Close the first client before REP replies
        req1.close

        # Give the connection loss time to propagate
        sleep 0.02

        # Reply to the now-dead client (should not crash)
        rep.send("reply-to-req1")

        # Second client sends a request — should still work
        req2 = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req2.connect("tcp://127.0.0.1:#{port}")

        req2.send("from-req2")
        msg = rep.receive
        assert_equal ["from-req2"], msg

        rep.send("reply-to-req2")
        reply = req2.receive
        assert_equal ["reply-to-req2"], reply
      ensure
        req1&.close
        req2&.close
        rep&.close
      end
    end
  end
end
