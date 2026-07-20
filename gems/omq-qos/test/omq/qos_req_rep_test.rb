# frozen_string_literal: true

require_relative "../test_helper"

describe "REQ send/recv ordering" do
  before { OMQ::Transport::Inproc.reset! }

  it "raises SocketError on double send" do
    Sync do
      rep = OMQ::REP.bind("ruby://req-order-1")
      req = OMQ::REQ.connect("ruby://req-order-1")

      req.send("first")
      assert_raises(SocketError) { req.send("second") }
    ensure
      req&.close
      rep&.close
    end
  end


  it "allows send after receive" do
    Sync do
      rep = OMQ::REP.bind("ruby://req-order-2")
      req = OMQ::REQ.connect("ruby://req-order-2")

      req.send("request1")
      rep.receive
      rep.send("reply1")
      assert_equal ["reply1"], req.receive

      req.send("request2")
      rep.receive
      rep.send("reply2")
      assert_equal ["reply2"], req.receive
    ensure
      req&.close
      rep&.close
    end
  end
end


describe "QoS 1 REQ/REP retry on disconnect" do
  it "re-sends to next REP when first drops" do
    Sync do
      rep1 = OMQ::REP.new
      rep1.qos = OMQ::QoS.at_least_once
      rep1.linger = 0
      port1 = rep1.bind("tcp://127.0.0.1:0").port

      rep2 = OMQ::REP.new
      rep2.qos = OMQ::QoS.at_least_once
      rep2.linger = 0
      port2 = rep2.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.new
      req.qos = OMQ::QoS.at_least_once
      req.linger             = 1
      req.reconnect_interval = RECONNECT_INTERVAL
      req.connect("tcp://127.0.0.1:#{port1}")
      req.connect("tcp://127.0.0.1:#{port2}")
      wait_connected(req)

      req.send("ping1")
      r = [rep1, rep2].find do |rep|
        Async::Task.current.with_timeout(0.5) { rep.receive } rescue nil
      end
      r.send("pong1")
      assert_equal ["pong1"], req.receive

      rep1.close
      rep1 = nil
      sleep 0.1

      req.send("ping2")
      assert_equal ["ping2"], rep2.receive
      rep2.send("pong2")
      assert_equal ["pong2"], req.receive
    ensure
      req&.close
      rep1&.close
      rep2&.close
    end
  end
end
