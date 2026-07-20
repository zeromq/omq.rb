# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 3 block API" do
  it "COMPs on block return, sender Promise resolves :delivered" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("payload")

      received = nil
      pull.receive { |parts| received = parts }

      result = Async::Task.current.with_timeout(2) { promise.wait }
      assert_equal ["payload"], received
      assert_equal :delivered, result
    ensure
      push&.close
      pull&.close
    end
  end


  it "NACKs on terminal error, sender dead-letters with NackInfo" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed(max_retries: 3)
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("bad")
      pull.receive { |_parts| raise OMQ::QoS::RejectedError, "no good" }

      result = Async::Task.current.with_timeout(2) { promise.wait }
      assert_kind_of OMQ::QoS::DeadLetter, result
      assert_equal :terminal_nack, result.reason
      assert_kind_of OMQ::QoS::NackInfo, result.error
      assert_equal OMQ::QoS::ErrorCodes::CODE_REJECTED, result.error.code
      assert_equal "no good", result.error.message
    ensure
      push&.close
      pull&.close
    end
  end


  it "retries on retryable NACK then dead-letters after max_retries" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      port = pull.bind("tcp://127.0.0.1:0").port

      max_retries = 2
      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed(
        max_retries:   max_retries,
        retry_backoff: (0.01..0.05),
      )
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("overloaded")

      attempts = 0
      max_retries.times do
        pull.receive do |_parts|
          attempts += 1
          raise OMQ::QoS::OverloadedError, "busy"
        end
      end

      result = Async::Task.current.with_timeout(3) { promise.wait }
      assert_equal max_retries, attempts
      assert_kind_of OMQ::QoS::DeadLetter, result
      assert_equal :retry_exhausted, result.reason
      assert_equal OMQ::QoS::ErrorCodes::CODE_OVERLOADED, result.error.code
    ensure
      push&.close
      pull&.close
    end
  end


  it "retries succeed after an initial NACK" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed(
        max_retries:   3,
        retry_backoff: (0.01..0.05),
      )
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("transient")

      first = true
      pull.receive do |_parts|
        if first
          first = false
          raise OMQ::QoS::OverloadedError, "busy"
        end
      end
      pull.receive { |_parts| :ok }

      result = Async::Task.current.with_timeout(3) { promise.wait }
      assert_equal :delivered, result
    ensure
      push&.close
      pull&.close
    end
  end
end
