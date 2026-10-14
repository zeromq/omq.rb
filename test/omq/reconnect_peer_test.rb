# frozen_string_literal: true

require_relative "../test_helper"
require "omq/peer"

# Disconnect-then-reconnect must work for PEER.
# The connecting side detects peer loss and auto-reconnects.
#
class OMQReconnectPeerTest < Minitest::Spec
  describe "Reconnect after server restart — PEER" do
    it "PEER" do
      Sync do
        a = OMQ::PEER.new(nil, linger: 0)
        port = a.bind("tcp://127.0.0.1:0").port

        b = OMQ::PEER.new(nil, linger: 0)
        b.reconnect_interval = RECONNECT_INTERVAL
        b.connect("tcp://127.0.0.1:#{port}")
        wait_connected(a, b)

        routing = a.engine.routing
        b_id    = routing.connections_by_routing_id.keys.first
        a.send_to(b_id, "hello")
        msg = b.receive
        assert_equal "hello", msg[1]

        a.close
        sleep 0.02

        a2 = OMQ::PEER.new(nil, linger: 0)
        a2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(a2, b)

        routing2 = a2.engine.routing
        b_id2    = routing2.connections_by_routing_id.keys.first
        a2.send_to(b_id2, "reconnected")
        msg = b.receive
        assert_equal "reconnected", msg[1]
      ensure
        b&.close
        a2&.close
      end
    end
  end
end
