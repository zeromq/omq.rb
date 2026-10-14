# frozen_string_literal: true

require_relative "../test_helper"

class OMQRecvFairnessTest < Minitest::Spec
  describe "recv pump fairness" do
    # Fairness only applies to IPC/TCP connections. Inproc uses
    # Pipe bypass which skips the recv pump entirely.

    it "interleaves messages from two fast IPC peers" do
      Async do |task|
        pull = OMQ::PULL.bind("ipc://@omq-test-fairness-#{$$}")

        push_a = OMQ::PUSH.connect("ipc://@omq-test-fairness-#{$$}")
        push_b = OMQ::PUSH.connect("ipc://@omq-test-fairness-#{$$}")
        wait_connected(push_a, push_b)

        n = 200

        # Pre-fill both kernel send buffers before any receive happens,
        # so both recv pumps have data ready simultaneously.
        barrier = Async::Barrier.new(parent: task)
        barrier.async { n.times { push_a.send("A") } }
        barrier.async { n.times { push_b.send("B") } }

        # Let both senders flush into the kernel buffer
        task.yield

        received = []
        (2 * n).times do
          Async::Task.current.with_timeout(5) do
            received << pull.receive.first
          end
        end

        barrier.wait

        # Both peers must be represented
        assert_equal n, received.count("A")
        assert_equal n, received.count("B")

        # Fairness: both peers must appear within the first
        # RECV_FAIRNESS_MESSAGES * 3 messages. Without the fairness
        # yield one peer could monopolize the recv queue.
        limit = OMQ::Engine::RECV_FAIRNESS_MESSAGES * 3
        first_window = received.first(limit)
        assert_includes first_window, "A", "peer A missing from first #{limit} messages"
        assert_includes first_window, "B", "peer B missing from first #{limit} messages"
      ensure
        push_a&.close
        push_b&.close
        pull&.close
      end
    end

    it "yields early when byte limit is reached (large messages)" do
      Async do
        pull = OMQ::PULL.bind("ipc://@omq-test-fairness-bytes-#{$$}")

        push_a = OMQ::PUSH.connect("ipc://@omq-test-fairness-bytes-#{$$}")
        push_b = OMQ::PUSH.connect("ipc://@omq-test-fairness-bytes-#{$$}")
        wait_connected(push_a, push_b)

        big   = "X" * (512 * 1024) # 512 KB per message — 2 messages hit the 1 MB byte limit
        small = "y"

        n_big   = 10
        n_small = 10

        barrier = Async::Barrier.new
        barrier.async { n_big.times { push_a.send(big) } }
        barrier.async { n_small.times { push_b.send(small) } }

        received = []
        (n_big + n_small).times do
          Async::Task.current.with_timeout(5) do
            received << pull.receive.first[0] # first char: "X" or "y"
          end
        end

        barrier.wait

        assert_equal n_big, received.count("X")
        assert_equal n_small, received.count("y")

        # With 512 KB messages, the byte limit (1 MB) triggers after ~2
        # messages. So "y" should appear within the first few messages,
        # not after all 10 big messages.
        first_window = received.first(n_big)
        assert_includes first_window, "y",
          "small-message peer starved by large-message peer"
      ensure
        push_a&.close
        push_b&.close
        pull&.close
      end
    end

    it "preserves per-connection message ordering" do
      Async do
        pull = OMQ::PULL.bind("ipc://@omq-test-fairness-order-#{$$}")

        push_a = OMQ::PUSH.connect("ipc://@omq-test-fairness-order-#{$$}")
        push_b = OMQ::PUSH.connect("ipc://@omq-test-fairness-order-#{$$}")
        wait_connected(push_a, push_b)

        n = 200

        barrier = Async::Barrier.new
        barrier.async { n.times { |i| push_a.send("A-#{i}") } }
        barrier.async { n.times { |i| push_b.send("B-#{i}") } }

        received = []
        (2 * n).times do
          Async::Task.current.with_timeout(5) do
            received << pull.receive.first
          end
        end

        barrier.wait

        # Messages from each peer must be in order (no reordering within
        # a connection), even though messages from different peers are
        # interleaved.
        a_msgs = received.select { |m| m.start_with?("A-") }
        b_msgs = received.select { |m| m.start_with?("B-") }

        assert_equal n.times.map { |i| "A-#{i}" }, a_msgs
        assert_equal n.times.map { |i| "B-#{i}" }, b_msgs
      ensure
        push_a&.close
        push_b&.close
        pull&.close
      end
    end
  end
end
