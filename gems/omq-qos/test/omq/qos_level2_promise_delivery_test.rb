# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 2 promise delivery" do
  it "resolves the Promise with :delivered on ACK" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "pull-A"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "push-A"
      push.qos                = OMQ::QoS.exactly_once
      push.linger             = 1
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("hello-qos2")
      assert_kind_of Async::Promise, promise
      assert_equal ["hello-qos2"], pull.receive

      result = Async::Task.current.with_timeout(2) { promise.wait }
      assert_equal :delivered, result
    ensure
      push&.close
      pull&.close
    end
  end


  it "resolves many Promises with :delivered under load" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "pull-many"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "push-many"
      push.qos                = OMQ::QoS.exactly_once
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      n = 20
      promises = Array.new(n) { |i| push.send("m-#{i}") }
      received = Array.new(n) { pull.receive.first }

      barrier = Async::Barrier.new
      results = Array.new(n)
      promises.each_with_index do |p, i|
        barrier.async { results[i] = p.wait }
      end
      Async::Task.current.with_timeout(5) { barrier.wait }

      assert_equal Array.new(n, :delivered), results
      assert_equal n, received.size
    ensure
      push&.close
      pull&.close
    end
  end
end
