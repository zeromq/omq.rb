# frozen_string_literal: true

require_relative "test_helper"

describe Protocol::ZMTP::PeerInfo do
  let(:public_key) { "pub-bytes" }
  let(:identity)   { "worker-A" }

  it "exposes public_key and identity" do
    info = Protocol::ZMTP::PeerInfo.new(public_key: public_key, identity: identity)

    assert_equal public_key, info.public_key
    assert_equal identity, info.identity
  end


  it "permits nil public_key (NULL mechanism) and empty identity" do
    info = Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "")

    assert_nil info.public_key
    assert_equal "", info.identity
  end


  it "is value-equal when fields match" do
    a = Protocol::ZMTP::PeerInfo.new(public_key: "k", identity: "i")
    b = Protocol::ZMTP::PeerInfo.new(public_key: "k", identity: "i")

    assert_equal a, b
    assert_equal a.hash, b.hash
  end


  it "differs when any field differs" do
    refute_equal(
      Protocol::ZMTP::PeerInfo.new(public_key: "k", identity: "i"),
      Protocol::ZMTP::PeerInfo.new(public_key: "k", identity: "j"),
    )
    refute_equal(
      Protocol::ZMTP::PeerInfo.new(public_key: "k", identity: "i"),
      Protocol::ZMTP::PeerInfo.new(public_key: "x", identity: "i"),
    )
  end


  it "is usable as a Hash key" do
    info_a = Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "A")
    info_b = Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "B")
    info_a2 = Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "A")

    h = { info_a => 1, info_b => 2 }
    h[info_a2] += 10

    assert_equal 11, h[info_a]
    assert_equal 2, h[info_b]
  end


  it "is frozen" do
    info = Protocol::ZMTP::PeerInfo.new(public_key: nil, identity: "x")

    assert_predicate info, :frozen?
  end
end
