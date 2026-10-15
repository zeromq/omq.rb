# frozen_string_literal: true

require_relative "../test_helper"

class OMQSendHwmReconnectTest < Minitest::Spec
  describe "PUSH with low send_hwm re-routes on disconnect" do
    it "delivers messages to a second peer after the first disconnects" do
      Async do
        push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
        push.send_hwm            = 1
        push.reconnect_interval  = RECONNECT_INTERVAL

        pull1 = OMQ::PULL.new.tap { |s| s.linger = 0 }
        port = pull1.bind("tcp://127.0.0.1:0").port

        push.connect("tcp://127.0.0.1:#{port}")
        wait_connected(push)

        # Fill the send queue and let consumer 1 read a few
        5.times { |i| push << "msg-#{i}" }
        r1 = 3.times.map { pull1.receive }
        assert_equal 3, r1.size

        # Kill consumer 1 — close the queue, triggering retry in enqueue
        pull1.close
        sleep 0.05

        # Consumer 2 on same port
        pull2 = OMQ::PULL.new.tap { |s| s.linger = 0 }
        pull2.recv_timeout = 3
        pull2.bind("tcp://127.0.0.1:#{port}")

        # Push more messages — these must reach consumer 2
        5.times { |i| push << "new-#{i}" }

        r2 = 5.times.map { pull2.receive }
        assert_equal 5, r2.size
        assert_equal ["new-0"], r2.first
      ensure
        [push, pull1, pull2].compact.each { |s| s.close rescue nil }
      end
    end
  end
end
