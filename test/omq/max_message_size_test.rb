# frozen_string_literal: true

require_relative "../test_helper"

class OMQMaxMessageSizeTest < Minitest::Spec
  describe "max_message_size" do
    it "rejects oversized frames over TCP" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.max_message_size = 10
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        # Small message — should work
        req.send("hi")
        msg = rep.receive
        assert_equal ["hi"], msg
        rep.send("ok")
        req.receive

        # Oversized message — connection should be dropped
        req.send("x" * 100)

        # REP should not receive it (connection dropped, reconnect loop)
        rep.read_timeout = 0.02
        assert_raises(IO::TimeoutError) { rep.receive }
      ensure
        req&.close
        rep&.close
      end
    end

    it "allows messages within limit" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.max_message_size = 1024
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        req.send("x" * 500)
        msg = rep.receive
        assert_equal 500, msg.first.bytesize
      ensure
        req&.close
        rep&.close
      end
    end

    it "accepts messages exactly at the limit" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.max_message_size = 100
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        req.send("x" * 100)
        msg = rep.receive
        assert_equal 100, msg.first.bytesize
      ensure
        req&.close
        rep&.close
      end
    end

    it "has no limit by default" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        assert_nil rep.max_message_size
        # Send well over the previous 1 MiB default to prove the limit is off.
        big = "x" * (4 << 20)
        req.send(big)
        msg = rep.receive
        assert_equal big.bytesize, msg.first.bytesize
      ensure
        req&.close
        rep&.close
      end
    end

    it "rejects when one frame in a multi-frame message exceeds limit" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.max_message_size = 50
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        # First frame ok, second exceeds limit — connection dropped
        req.send(["small", "x" * 100])

        rep.read_timeout = 0.02
        assert_raises(IO::TimeoutError) { rep.receive }
      ensure
        req&.close
        rep&.close
      end
    end
  end
end
