# frozen_string_literal: true

require_relative "../test_helper"

describe OMQ::Socket do
  before { OMQ::Transport::Inproc.reset! }

  describe "#inspect" do
    it "includes class name and bound endpoints" do
      Async do
        rep = OMQ::REP.bind("ruby://inspect-test")
        s = rep.inspect
        assert_match(/OMQ::REP/, s)
        assert_match(/ruby:\/\/inspect-test/, s)
      ensure
        rep&.close
      end
    end

    it "shows empty bound list before bind/connect" do
      Async do
        rep = OMQ::REP.new
        assert_match(/bound=\[\]/, rep.inspect)
      ensure
        rep&.close
      end
    end
  end

  describe "ØMQ alias" do
    it "is the same as OMQ" do
      assert_equal OMQ, ØMQ
      assert_equal OMQ::REQ, ØMQ::REQ
      assert_equal OMQ::PUB, ØMQ::PUB
    end
  end

  describe "options" do
    it "exposes every core option through socket accessors" do
      socket = OMQ::PUSH.new
      mechanism = Protocol::ZMTP::Mechanism::Null.new

      options = {
        send_hwm: 10,
        recv_hwm: 11,
        linger: 0,
        identity: "sock-1",
        read_timeout: 0.1,
        write_timeout: 0.2,
        router_mandatory: true,
        reconnect_interval: 0.3,
        heartbeat_interval: 0.4,
        heartbeat_ttl: 0.5,
        heartbeat_timeout: 0.6,
        max_message_size: 42,
        conflate: true,
        sndbuf: 1024,
        rcvbuf: 2048,
        on_mute: :drop_newest,
        mechanism: mechanism,
      }

      options.each do |name, value|
        socket.public_send("#{name}=", value)

        assert_equal value, socket.public_send(name)
      end

      socket.recv_timeout = 0.7
      socket.send_timeout = 0.8

      assert_equal 0.7, socket.recv_timeout
      assert_equal 0.7, socket.read_timeout
      assert_equal 0.8, socket.send_timeout
      assert_equal 0.8, socket.write_timeout
    ensure
      socket&.close
    end
  end

  describe "empty and binary messages" do
    it "handles empty string message" do
      Async do
        pull = OMQ::PULL.bind("ruby://empty-msg")
        push = OMQ::PUSH.connect("ruby://empty-msg")

        push.send("")
        msg = pull.receive
        assert_equal [""], msg
      ensure
        push&.close
        pull&.close
      end
    end

    it "handles binary data with all 256 byte values" do
      Async do
        pull = OMQ::PULL.bind("ruby://binary-msg")
        push = OMQ::PUSH.connect("ruby://binary-msg")

        binary = (0..255).map(&:chr).join.b
        push.send(binary)
        msg = pull.receive
        assert_equal [binary], msg
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
