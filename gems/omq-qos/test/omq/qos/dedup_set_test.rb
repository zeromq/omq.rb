# frozen_string_literal: true

require_relative "../../test_helper"

describe OMQ::QoS::DedupSet do
  let(:set) { OMQ::QoS::DedupSet.new(capacity: 3) }

  it "is empty by default" do
    assert set.empty?
    assert_equal 0, set.size
    refute set.seen?("abc12345")
  end


  it "remembers added digests" do
    set.add("aaaaaaaa")
    assert set.seen?("aaaaaaaa")
    assert_equal 1, set.size
  end


  it "forgets removed digests" do
    set.add("aaaaaaaa")
    set.remove("aaaaaaaa")
    refute set.seen?("aaaaaaaa")
    assert set.empty?
  end


  it "evicts oldest on capacity overflow" do
    set.add("11111111")
    set.add("22222222")
    set.add("33333333")
    set.add("44444444")

    refute set.seen?("11111111")
    assert set.seen?("22222222")
    assert set.seen?("33333333")
    assert set.seen?("44444444")
    assert_equal 3, set.size
  end


  it "sweeps entries older than ttl" do
    Sync do
      set.add("aaaaaaaa")
      sleep 0.05
      now = Async::Clock.now
      set.add("bbbbbbbb")

      set.sweep(now, 0.01)
      refute set.seen?("aaaaaaaa")
      assert set.seen?("bbbbbbbb")
    end
  end
end
