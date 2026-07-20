# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 3 #each block threading" do
  it "threads the block through #receive for each iteration" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity    = "server"
      pull.qos         = OMQ::QoS.exactly_once_and_processed
      pull.read_timeout = 0.05
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.identity           = "client"
      push.qos                = OMQ::QoS.exactly_once_and_processed
      push.linger             = 2
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      promises = 3.times.map { |i| push.send("m-#{i}") }

      received = []
      pull.each { |parts| received << parts.first }

      promises.each { |p| Async::Task.current.with_timeout(2) { p.wait } }
      assert_equal ["m-0", "m-1", "m-2"], received
    ensure
      push&.close
      pull&.close
    end
  end


  it "NACKs when the block raises; #each continues" do
    Sync do
      pull = OMQ::PULL.new
      pull.identity    = "server"
      pull.qos         = OMQ::QoS.exactly_once_and_processed
      pull.read_timeout = 0.1
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

      p1 = push.send("bad")
      p2 = push.send("good")

      seen = []
      pull.each do |parts|
        seen << parts.first
        raise OMQ::QoS::RejectedError, "no" if parts.first == "bad"
      end

      r1 = Async::Task.current.with_timeout(2) { p1.wait }
      r2 = Async::Task.current.with_timeout(2) { p2.wait }

      assert_includes seen, "bad"
      assert_includes seen, "good"
      assert_kind_of OMQ::QoS::DeadLetter, r1
      assert_equal :terminal_nack, r1.reason
      assert_equal :delivered, r2
    ensure
      push&.close
      pull&.close
    end
  end
end
