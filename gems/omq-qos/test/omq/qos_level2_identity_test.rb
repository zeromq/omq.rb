# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 2 identity validation" do
  it "accepts an anonymous connection at QoS 1" do
    Sync do
      pull = OMQ::PULL.new
      pull.qos = OMQ::QoS.at_least_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.qos                = OMQ::QoS.at_least_once
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      push.send("hi")
      assert_equal ["hi"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "rejects an anonymous peer at QoS 2" do
    Sync do
      pull = OMQ::PULL.new
      pull.qos = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.qos                = OMQ::QoS.exactly_once
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")

      # Handshake should fail — neither side set identity or CURVE.
      # peer_connected never resolves.
      assert_raises(Async::TimeoutError) do
        Async::Task.current.with_timeout(0.1) { push.peer_connected.wait }
      end
    ensure
      push&.close
      pull&.close
    end
  end


  it "accepts a peer with ZMQ_IDENTITY at QoS 2" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      push.send("ok")
      assert_equal ["ok"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end
end
