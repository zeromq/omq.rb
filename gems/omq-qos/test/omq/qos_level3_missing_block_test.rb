# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 3 missing block" do
  it "raises ArgumentError when #receive has no block at level 3" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      pull.bind("tcp://127.0.0.1:0")

      assert_raises(ArgumentError) { pull.receive }
    ensure
      pull&.close
    end
  end


  it "raises ArgumentError when #each has no block at level 3" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      pull.bind("tcp://127.0.0.1:0")

      assert_raises(ArgumentError) { pull.each }
    ensure
      pull&.close
    end
  end
end
