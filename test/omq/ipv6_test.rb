# frozen_string_literal: true

require_relative "../test_helper"

class OMQIpv6Test < Minitest::Spec
  describe "IPv6" do
    def ipv6_available?
      s = TCPServer.new("::1", 0)
      s.close
      true
    rescue
      false
    end

    before do
      skip "IPv6 not available on this system" unless ipv6_available?
    end

    it "REQ/REP over TCP with ::1" do
      Async do
        rep = OMQ::REP.new
        port = rep.bind("tcp://[::1]:0").port

        req = OMQ::REQ.connect("tcp://[::1]:#{port}")

        req.send("hello ipv6")
        msg = rep.receive
        assert_equal ["hello ipv6"], msg

        rep.send("world ipv6")
        reply = req.receive
        assert_equal ["world ipv6"], reply
      ensure
        req&.close
        rep&.close
      end
    end

    it "PUSH/PULL over TCP with [::1]" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://[::1]:0").port

        push = OMQ::PUSH.connect("tcp://[::1]:#{port}")

        push.send("ipv6 pipeline")
        msg = pull.receive
        assert_equal ["ipv6 pipeline"], msg
      ensure
        push&.close
        pull&.close
      end
    end

    it "PUB/SUB over TCP with ::1" do
      Async do
        pub = OMQ::PUB.new
        port = pub.bind("tcp://[::1]:0").port

        sub = OMQ::SUB.connect("tcp://[::1]:#{port}", subscribe: "topic.")

        # Wait for subscription to propagate
        pub.subscriber_joined.wait

        pub.send("topic.data")
        msg = sub.receive
        assert_equal ["topic.data"], msg
      ensure
        sub&.close
        pub&.close
      end
    end

    it "ephemeral port works with IPv6" do
      Async do
        rep = OMQ::REP.new
        uri = rep.bind("tcp://[::1]:0")
        port = uri.port
        refute_nil port
        assert port > 0
        assert_equal "tcp://[::1]:#{port}", uri.to_s
      ensure
        rep&.close
      end
    end
  end
end
