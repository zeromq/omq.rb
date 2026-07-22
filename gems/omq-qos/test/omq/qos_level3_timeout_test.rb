# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 3 processing_timeout" do
  it "NACKs with TIMEOUT code when block exceeds processing_timeout" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity = "server"
      pull.qos      = OMQ::QoS.exactly_once_and_processed(processing_timeout: 0.05)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed(
        max_retries:   1,
        retry_backoff: (0.01..0.02),
      )
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promise = push.send("slow")

      pull.receive { |_parts| sleep 0.3 }

      result = Async::Task.current.with_timeout(2) { promise.wait }
      assert_kind_of OMQ::QoS::DeadLetter, result
      assert_equal :retry_exhausted, result.reason
      assert_equal OMQ::QoS::ErrorCodes::CODE_TIMEOUT, result.error.code
    ensure
      push&.close
      pull&.close
    end
  end
end
