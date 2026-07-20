# frozen_string_literal: true

require_relative "../../test_helper"
require "async/promise"

describe OMQ::QoS::PeerRegistry do
  let(:registry) { OMQ::QoS::PeerRegistry.new(capacity: 1000) }
  let(:peer_a)   { Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "A") }
  let(:peer_b)   { Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "B") }
  let(:conn_a)   { Object.new }
  let(:conn_b)   { Object.new }


  def make_entry(peer_info: peer_a, parts: ["m"].freeze, retry_count: 0)
    OMQ::QoS::PeerRegistry::Entry.new(
      parts:       parts,
      peer_info:   peer_info,
      sent_at:     Async::Clock.now,
      promise:     Async::Promise.new,
      retry_count: retry_count,
    )
  end


  it "tracks and acks entries per peer" do
    registry.track(peer_a, "h1______", make_entry, conn_a)
    assert_equal 1, registry.size

    entry = registry.ack(peer_a, "h1______")
    refute_nil entry
    assert_equal 0, registry.size
    assert registry.empty?
  end


  it "returns nil for unknown digest" do
    assert_nil registry.ack(peer_a, "missing_")
    registry.track(peer_a, "h1______", make_entry, conn_a)
    assert_nil registry.ack(peer_a, "missing_")
    assert_nil registry.ack(peer_b, "h1______")
  end


  it "resume replays entries in insertion order, ignoring other peers" do
    registry.track(peer_a, "h1______", make_entry(parts: ["a1"].freeze), conn_a)
    registry.track(peer_b, "h2______", make_entry(peer_info: peer_b, parts: ["b1"].freeze), conn_b)
    registry.track(peer_a, "h3______", make_entry(parts: ["a2"].freeze), conn_a)

    replay = registry.resume(peer_a, conn_a)
    assert_equal 2, replay.size
    assert_equal ["a1"], replay[0].parts
    assert_equal ["a2"], replay[1].parts
  end


  it "disconnect holds entries (no drop)" do
    registry.track(peer_a, "h1______", make_entry, conn_a)
    registry.disconnect(peer_a)

    assert_equal 1, registry.size
    replay = registry.resume(peer_a, conn_a)
    assert_equal 1, replay.size
  end


  it "sweeps dead letters after timeout" do
    Sync do
      e1 = make_entry(parts: ["old"].freeze)
      registry.track(peer_a, "h1______", e1, conn_a)
      registry.disconnect(peer_a)
      sleep 0.05

      expired = registry.sweep_dead_letters(Async::Clock.now, 0.01)
      assert_equal 1, expired.size

      entry, pinfo = expired[0]
      assert_equal e1, entry
      assert_equal peer_a, pinfo
      assert_equal 0, registry.size
    end
  end


  it "sweep leaves still-connected peers alone" do
    registry.track(peer_a, "h1______", make_entry, conn_a)

    expired = registry.sweep_dead_letters(Async::Clock.now + 3600, 0.01)
    assert_equal 0, expired.size
    assert_equal 1, registry.size
  end


  it "drain_with_dead_letter resolves all pending promises" do
    e1 = make_entry(parts: ["m1"].freeze)
    e2 = make_entry(peer_info: peer_b, parts: ["m2"].freeze)
    registry.track(peer_a, "h1______", e1, conn_a)
    registry.track(peer_b, "h2______", e2, conn_b)

    registry.drain_with_dead_letter(:socket_closed)

    [e1, e2].each do |entry|
      assert entry.promise.resolved?
      dl = entry.promise.value
      assert_kind_of OMQ::QoS::DeadLetter, dl
      assert_equal :socket_closed, dl.reason
    end
    assert registry.empty?
  end
end
