# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 2 peer pinning" do
  it "does NOT failover to another peer on disconnect" do
    Sync do
      pull_a = OMQ::PULL.new
      pull_a.identity = "A"
      pull_a.qos      = OMQ::QoS.exactly_once
      pull_a.recv_hwm = 1
      port_a = pull_a.bind("tcp://127.0.0.1:0").port

      pull_b = OMQ::PULL.new
      pull_b.identity = "B"
      pull_b.qos      = OMQ::QoS.exactly_once
      port_b = pull_b.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "sender"
      push.qos                = OMQ::QoS.exactly_once(dead_letter_timeout: 60)
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port_a}")
      push.connect("tcp://127.0.0.1:#{port_b}")
      wait_connected(push)

      # Send enough messages so both peers are used by round-robin.
      n = 10
      promises = Array.new(n) { |i| push.send("m-#{i}") }

      # Drain what pull_b receives before closing A — only the
      # round-robin slice for B. Leave pull_a's slice stuck in push's
      # pending registry (B received + app-consumed, so B's slice is
      # ACK'd; A's slice is in-flight).
      received_b = []
      pull_b.read_timeout = 0.1
      loop do
        received_b << pull_b.receive.first
        break if received_b.size >= n / 2
      rescue IO::TimeoutError
        break
      end

      # Kill A without consuming → A's pending stays pinned to A.
      pull_a.close
      pull_a = nil

      # Nothing further is delivered to B — QoS 2 does NOT failover.
      pull_b.read_timeout = 0.1
      extra_on_b = []
      loop do
        extra_on_b << pull_b.receive.first
      rescue IO::TimeoutError
        break
      end

      assert promises.any? { !it.resolved? },
             "expected at least one Promise to remain pending (pinned to A)"
    ensure
      push&.close
      pull_a&.close
      pull_b&.close
    end
  end


  it "replays pinned messages when the same peer returns" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once(dead_letter_timeout: 60)
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise_a = push.send("a")
      assert_equal ["a"], pull.receive
      Async::Task.current.with_timeout(2) { promise_a.wait }

      # Bounce pull — close and re-bind on the same port with the
      # same identity. Pending messages on push (if any) must replay.
      pull.close
      sleep 0.05

      pull2 = OMQ::PULL.new
      pull2.identity = "server"
      pull2.qos      = OMQ::QoS.exactly_once
      pull2.bind("tcp://127.0.0.1:#{port}")
      wait_connected(push, timeout: 3)

      promise_b = push.send("b")
      assert_equal ["b"], pull2.receive
      result = Async::Task.current.with_timeout(2) { promise_b.wait }
      assert_equal :delivered, result
    ensure
      push&.close
      pull&.close
      pull2&.close
    end
  end
end
