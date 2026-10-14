# frozen_string_literal: true

require_relative "../../test_helper"

class OMQTransportTCPTest < Minitest::Spec
  describe "TCP transport" do
    it "advertises Protocol::ZMTP::Connection as its connection_class" do
      assert_equal Protocol::ZMTP::Connection, OMQ::Transport::TCP.connection_class
    end


    it "PAIR over TCP with ephemeral port" do
      Async do
        server = OMQ::PAIR.new
        port = server.bind("tcp://127.0.0.1:0").port
        refute_nil port
        assert port > 0

        client = OMQ::PAIR.connect("tcp://127.0.0.1:#{port}")

        client.send("hello tcp")
        msg = server.receive
        assert_equal ["hello tcp"], msg

        server.send("reply tcp")
        msg = client.receive
        assert_equal ["reply tcp"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "REQ/REP over TCP" do
      Async do
        rep = OMQ::REP.new
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.connect("tcp://127.0.0.1:#{port}")

        req.send("request")
        request = rep.receive
        assert_equal ["request"], request

        rep.send("reply")
        reply = req.receive
        assert_equal ["reply"], reply
      ensure
        req&.close
        rep&.close
      end
    end

    it "PUSH/PULL over TCP" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")

        push.send("pipeline msg")
        msg = pull.receive
        assert_equal ["pipeline msg"], msg
      ensure
        push&.close
        pull&.close
      end
    end

    describe "host normalization" do
      it "normalize_bind_host maps * to nil (dual-stack wildcard)" do
        assert_nil OMQ::Transport::TCP.normalize_bind_host("*")
      end

      it "normalize_bind_host maps localhost/empty/nil to loopback_host" do
        lb = OMQ::Transport::TCP.loopback_host
        assert_equal lb, OMQ::Transport::TCP.normalize_bind_host("localhost")
        assert_equal lb, OMQ::Transport::TCP.normalize_bind_host("")
        assert_equal lb, OMQ::Transport::TCP.normalize_bind_host(nil)
      end

      it "normalize_bind_host passes through explicit addresses" do
        assert_equal "0.0.0.0", OMQ::Transport::TCP.normalize_bind_host("0.0.0.0")
        assert_equal "::",      OMQ::Transport::TCP.normalize_bind_host("::")
        assert_equal "10.1.2.3", OMQ::Transport::TCP.normalize_bind_host("10.1.2.3")
      end

      it "normalize_connect_host maps *, empty, nil, localhost to loopback_host" do
        lb = OMQ::Transport::TCP.loopback_host
        assert_equal lb, OMQ::Transport::TCP.normalize_connect_host("*")
        assert_equal lb, OMQ::Transport::TCP.normalize_connect_host("")
        assert_equal lb, OMQ::Transport::TCP.normalize_connect_host(nil)
        assert_equal lb, OMQ::Transport::TCP.normalize_connect_host("localhost")
      end

      it "normalize_connect_host passes through real hostnames" do
        assert_equal "example.com", OMQ::Transport::TCP.normalize_connect_host("example.com")
        assert_equal "127.0.0.1",   OMQ::Transport::TCP.normalize_connect_host("127.0.0.1")
      end

      it "loopback_host returns ::1 or 127.0.0.1" do
        assert_includes ["::1", "127.0.0.1"], OMQ::Transport::TCP.loopback_host
      end
    end

    it "binds tcp://*:0 as dual-stack and accepts IPv4 and IPv6 loopback connects" do
      Async do
        pull = OMQ::PULL.new
        uri  = pull.bind("tcp://*:0")
        port = uri.port
        assert_match %r{\Atcp://\*:\d+\z}, uri.to_s

        push4 = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")
        push4.send("via-ipv4")
        assert_equal ["via-ipv4"], pull.receive

        push6 = OMQ::PUSH.connect("tcp://[::1]:#{port}")
        push6.send("via-ipv6")
        assert_equal ["via-ipv6"], pull.receive
      ensure
        push4&.close
        push6&.close
        pull&.close
      end
    end

    it "binds tcp://localhost:0 and tcp://:0 to the preferred loopback family" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://localhost:0").port

        push = OMQ::PUSH.connect("tcp://localhost:#{port}")
        push.send("loopback msg")
        assert_equal ["loopback msg"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end

    it "connect normalizes tcp://localhost and tcp://: to loopback" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://*:0").port

        push_localhost = OMQ::PUSH.connect("tcp://localhost:#{port}")
        push_localhost.send("from-localhost")
        assert_equal ["from-localhost"], pull.receive

        push_empty = OMQ::PUSH.connect("tcp://:#{port}")
        push_empty.send("from-empty")
        assert_equal ["from-empty"], pull.receive
      ensure
        push_localhost&.close
        push_empty&.close
        pull&.close
      end
    end
  end
end
