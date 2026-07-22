# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 2 dedup" do
  it "delivers each message exactly once to the application" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      n        = 20
      promises = Array.new(n) { |i| push.send("m-#{i}") }
      received = Array.new(n) { pull.receive.first }

      barrier = Async::Barrier.new
      promises.each { |p| barrier.async { p.wait } }
      Async::Task.current.with_timeout(5) { barrier.wait }

      expected = (0...n).map { |i| "m-#{i}" }
      assert_equal expected, received
    ensure
      push&.close
      pull&.close
    end
  end


  it "dedup set clears entries on CLR from sender" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      p1 = push.send("payload")
      pull.receive
      Async::Task.current.with_timeout(2) { p1.wait }

      # Wait for CLR round-trip to drain pull's dedup set.
      sleep 0.1

      pull_qos    = pull.qos
      dedup_sets  = pull_qos.instance_variable_get(:@dedup_sets)
      total       = (dedup_sets&.values || []).sum(&:size)
      assert_equal 0, total, "dedup set should be empty after CLR"
    ensure
      push&.close
      pull&.close
    end
  end
end
