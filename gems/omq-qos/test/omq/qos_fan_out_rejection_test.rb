# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS rejects fan-out socket types" do
  it "raises when setting qos on PUB" do
    pub = OMQ::PUB.new
    err = assert_raises(ArgumentError) { pub.qos = OMQ::QoS.at_least_once }
    assert_match(/PUB/, err.message)
  ensure
    pub&.close
  end


  it "raises when setting qos on SUB" do
    sub = OMQ::SUB.new
    err = assert_raises(ArgumentError) { sub.qos = OMQ::QoS.at_least_once }
    assert_match(/SUB/, err.message)
  ensure
    sub&.close
  end


  it "raises when setting qos on XPUB" do
    xpub = OMQ::XPUB.new
    assert_raises(ArgumentError) { xpub.qos = OMQ::QoS.exactly_once }
  ensure
    xpub&.close
  end


  it "raises when setting qos on XSUB" do
    xsub = OMQ::XSUB.new
    assert_raises(ArgumentError) { xsub.qos = OMQ::QoS.at_least_once }
  ensure
    xsub&.close
  end


  it "still allows qos = nil on fan-out types" do
    pub = OMQ::PUB.new
    pub.qos = nil
    assert_nil pub.qos
  ensure
    pub&.close
  end


  it "still allows qos on point-to-point types" do
    push = OMQ::PUSH.new
    push.qos = OMQ::QoS.at_least_once
    assert_kind_of OMQ::QoS, push.qos
    assert_equal 1, push.qos.level
  ensure
    push&.close
  end


  it "raises on Integer" do
    push = OMQ::PUSH.new
    err = assert_raises(ArgumentError) { push.qos = 1 }
    assert_match(/OMQ::QoS instance/, err.message)
  ensure
    push&.close
  end


  it "raises on other types" do
    push = OMQ::PUSH.new
    assert_raises(ArgumentError) { push.qos = "at_least_once" }
    assert_raises(ArgumentError) { push.qos = :at_least_once }
  ensure
    push&.close
  end
end
