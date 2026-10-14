# frozen_string_literal: true

require_relative "../test_helper"

class OMQLingerTest < Minitest::Spec
  describe "Linger" do
    it "drains send queue before closing when linger > 0" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
        push.connect("tcp://127.0.0.1:#{port}")

        # Send several messages
        5.times { |i| push.send("msg-#{i}") }

        # Close with linger — should wait for messages to drain
        push.close

        # All messages should have been delivered
        5.times do |i|
          msg = pull.receive
          assert_equal ["msg-#{i}"], msg
        end
      ensure
        pull&.close
      end
    end

    it "closes immediately when linger = 0" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
        push.connect("tcp://127.0.0.1:#{port}")

        push.send("before close")

        # Close immediately — some messages may be lost
        elapsed = Async::Clock.measure do
          push.close
        end

        # Close should be near-instant (< 100ms)
        assert_operator elapsed, :<, 0.1
      ensure
        pull&.close
      end
    end

    it "reconnects during linger to deliver queued messages" do
      Async do
        # Grab a free port by binding temporarily
        tmp = TCPServer.new("127.0.0.1", 0)
        port = tmp.local_address.ip_port
        tmp.close

        push = OMQ::PUSH.new.tap { |s| s.linger = 5 }
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{port}")

        # Send while no peer is listening — message queues
        push.send("early")

        # Bind after a delay — reconnect should find it during linger
        sleep 0.02
        pull = OMQ::PULL.bind("tcp://127.0.0.1:#{port}")

        # Close push — linger should drain the queued message
        push.close

        pull.recv_timeout = 2
        msg = pull.receive
        assert_equal ["early"], msg
      ensure
        pull&.close
      end
    end

    it "delivers multiple messages when peer appears during linger period" do
      Async do
        tmp = TCPServer.new("127.0.0.1", 0)
        port = tmp.local_address.ip_port
        tmp.close

        push = OMQ::PUSH.new.tap { |s| s.linger = 5 }
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{port}")

        3.times { |i| push.send("msg-#{i}") }

        # Start close in a separate task — it will block during linger
        closer = Async { push.close }

        # Bind while close is draining
        sleep 0.02
        pull = OMQ::PULL.bind("tcp://127.0.0.1:#{port}")

        closer.wait

        pull.recv_timeout = 2
        received = []
        3.times { received << pull.receive.first }
        assert_equal ["msg-0", "msg-1", "msg-2"], received
      ensure
        pull&.close
      end
    end

    it "drains in-flight messages across multiple peers before closing" do
      Async do
        pull_a = OMQ::PULL.new
        pull_b = OMQ::PULL.new
        port_a = pull_a.bind("tcp://127.0.0.1:0").port
        port_b = pull_b.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 5 }
        push.connect("tcp://127.0.0.1:#{port_a}")
        push.connect("tcp://127.0.0.1:#{port_b}")

        # Wait for both peers so pumps exist for both connections.
        sleep 0.001 until push.connection_count >= 2

        # Large messages (64KB each) force IO yields during write_batch,
        # opening a window where pump fibers have dequeued messages but
        # haven't flushed them yet. This is the race condition:
        # drain_send_queues sees an empty queue while pumps hold in-flight
        # batches, proceeds to tear_down_barrier, and kills them.
        n       = 20
        payload = "X" * 65_536
        n.times { |i| push.send(["#{i}:#{payload}"]) }
        push.close

        # Collect from both pulls — every message must arrive.
        received = []
        barrier  = Async::Barrier.new
        [pull_a, pull_b].each do |pull|
          barrier.async do
            loop do
              msg = pull.receive
              received << msg.first.split(":").first.to_i
              barrier.stop if received.size >= n
            end
          end
        end
        barrier.wait

        assert_equal n, received.size, "expected all #{n} messages delivered, got #{received.size}"
      ensure
        pull_a&.close
        pull_b&.close
      end
    end

    it "aborts drain cleanly when the close task is stopped mid-linger" do
      # Regression: drain_send_queues used to propagate Async::Stop out of
      # close when the enclosing task was cancelled, leaving teardown
      # half-done. It now rescues Async::Stop so close can finish the
      # rest of its teardown.
      Async do
        # No peer — the message sits in the send queue forever, so
        # drain_send_queues would otherwise spin until linger expires.
        push = OMQ::PUSH.new(nil, send_hwm: 1, linger: Float::INFINITY)
        push.bind("ipc://@omq-test-drain-cancel")
        push.send("stuck")

        closer = Async { push.close }
        sleep 0.01 # let close enter drain_send_queues' sleep loop

        elapsed = Async::Clock.measure do
          closer.stop
          closer.wait
        end

        # Stop should unblock the drain immediately rather than hanging
        # on Float::INFINITY linger, and must not propagate out.
        assert_operator elapsed, :<, 0.1
      end
    end

    it "actually delivers all messages before close completes over TCP" do
      Async do
        pull = OMQ::PULL.new
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 2 }
        push.connect("tcp://127.0.0.1:#{port}")
        wait_connected(push, pull)

        20.times { |i| push.send("drain-#{i}") }
        push.close

        received = []
        20.times do
          pull.recv_timeout = 1
          received << pull.receive.first
        end

        assert_equal 20, received.size
        assert_equal "drain-0", received.first
        assert_equal "drain-19", received.last
      ensure
        pull&.close
      end
    end
  end
end
