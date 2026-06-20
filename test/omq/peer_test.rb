# frozen_string_literal: true

require_relative "../test_helper"
require "omq/peer"

describe "PEER over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  it "routes messages by routing ID" do
    Sync do |task|
      a = OMQ::PEER.bind("ruby://peer-1")
      b = OMQ::PEER.connect("ruby://peer-1")

      # b sends to a. b only has one peer, so send_to needs b's
      # routing_id for that peer. But routing IDs are assigned by
      # the receiver's routing strategy. We need another way to
      # discover it. Use last_routing_id after first receive.

      # a sends a "ping" using its routing_id for b (assigned in connection_added).
      # We expose this via a helper on the routing.
      a_routing = a.engine.routing
      b_id_on_a = a_routing.connections_by_routing_id.keys.first

      a.send_to(b_id_on_a, "ping from a")
      msg = b.receive
      a_id_on_b = msg[0]
      assert_equal "ping from a", msg[1]

      b.send_to(a_id_on_b, "pong from b")
      msg = a.receive
      assert_equal b_id_on_a, msg[0]
      assert_equal "pong from b", msg[1]
    ensure
      a&.close
      b&.close
    end
  end

  it "rejects multipart messages via send" do
    Sync do
      a = OMQ::PEER.bind("ruby://peer-mp")
      b = OMQ::PEER.connect("ruby://peer-mp")

      assert_raises(ArgumentError) { b.send(["part1", "part2"]) }
    ensure
      a&.close
      b&.close
    end
  end
end
