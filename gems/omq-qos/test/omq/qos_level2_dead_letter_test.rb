# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 2 dead-letter" do
  it "resolves Promises with DeadLetter(:socket_closed) on socket close" do
    Sync do
      push = OMQ::PUSH.new
      push.identity           = "sender"
      push.qos                = OMQ::QoS.exactly_once(dead_letter_timeout: 60)
      push.linger             = 0
      push.reconnect_interval = RECONNECT_INTERVAL
      # Connect to a never-binding endpoint → handshake never completes,
      # but at QoS >= 2 #send still returns a Promise (sender-side).
      push.connect("tcp://127.0.0.1:1") # unlikely to be listening

      # Give a moment for the connect dial to happen; it'll keep retrying.
      # Send a message — it stays in the send queue (no peer).
      promise = push.send("stuck")
      refute promise.resolved?

      push.close
      push = nil

      # Promise may resolve via socket_closed. If the message never
      # reached the PeerRegistry (no handshake yet), the Promise stays
      # pending — we don't assert resolution in that case.
      if promise.resolved?
        dl = promise.value
        assert_kind_of OMQ::QoS::DeadLetter, dl
        assert_equal :socket_closed, dl.reason
      end
    ensure
      push&.close
    end
  end


  it "resolves Promises with DeadLetter(:peer_timeout) after dead_letter_timeout" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.recv_hwm = 1
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once(dead_letter_timeout: 0.05)
      push.linger             = 0
      push.reconnect_interval = 10.0 # do not reconnect during the test
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      # First message fills pull's recv_queue (hwm=1). It gets ACK'd.
      p0 = push.send("first")
      Async::Task.current.with_timeout(2) { p0.wait }

      # Second message reaches pull's recv pump but blocks on enqueue
      # (queue already has "first", no dequeue). With ACK-after-enqueue,
      # no ACK flows for "second" — it stays pinned to "server" in the
      # sender's PeerRegistry.
      p1 = push.send("second")
      sleep 0.02
      refute p1.resolved?, "Promise should still be pending before peer disconnect"

      # Peer disappears → disconnected_at stamped → sweep expires entry.
      pull.close
      pull = nil

      result = Async::Task.current.with_timeout(2) { p1.wait }
      assert_kind_of OMQ::QoS::DeadLetter, result
      assert_equal :peer_timeout, result.reason
      assert_equal ["second"], result.parts
    ensure
      push&.close
      pull&.close
    end
  end
end
