# frozen_string_literal: true

require_relative "../test_helper"
require "pathname"

describe "PUSH/PULL over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  it "sends and receives messages" do
    Async do
      pull = OMQ::PULL.bind("ruby://pushpull-1")
      push = OMQ::PUSH.connect("ruby://pushpull-1")

      push.send("hello")
      msg = pull.receive
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end

  it "distributes messages across multiple PULL peers" do
    Async do
      pull1 = OMQ::PULL.new
      pull2 = OMQ::PULL.new
      pull1.read_timeout = 0.2
      pull2.read_timeout = 0.2
      port1 = pull1.bind("tcp://127.0.0.1:0").port
      port2 = pull2.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port1}")
      push.connect("tcp://127.0.0.1:#{port2}")

      # Wait for both peers to be handshake-complete before sending,
      # otherwise the first pump that comes up may absorb all 1000
      # messages into its TCP buffer before the second has connected.
      Async::Task.current.with_timeout(2) do
        sleep 0.001 until push.connection_count >= 2
      end

      n = 1000
      n.times { |i| push.send("msg-#{i}") }

      received = []
      barrier  = Async::Barrier.new
      [pull1, pull2].each do |pull|
        barrier.async do
          loop do
            msg = pull.receive
            received << [pull, msg.first]
          rescue IO::TimeoutError
            break
          end
        end
      end
      barrier.wait

      # Work-stealing distributes; both peers should get some load,
      # but the split is not strict round-robin.
      assert_equal n, received.size
      assert_equal n, received.map(&:last).uniq.size
      from_pull1 = received.count { |pull, _| pull == pull1 }
      from_pull2 = received.count { |pull, _| pull == pull2 }
      assert from_pull1 > 0, "expected pull1 to receive some messages"
      assert from_pull2 > 0, "expected pull2 to receive some messages"
    ensure
      push&.close
      pull1&.close
      pull2&.close
    end
  end
  it "distributes across peers in recv-then-send loop (pipe pattern)" do
    skip "flaky on CI — hangs under the GHA scheduler, passes locally" if ENV["CI"]
    Async do
      # Source → pipe_pull → pipe_push → [sink_a, sink_b]
      pipe_pull = OMQ::PULL.bind("ipc://@omq_test_pipe_in_#{$$}")
      sink_a    = OMQ::PULL.bind("ipc://@omq_test_pipe_out_a_#{$$}")
      sink_b    = OMQ::PULL.bind("ipc://@omq_test_pipe_out_b_#{$$}")

      source = OMQ::PUSH.new.tap { |s| s.linger = 5 }
      source.connect("ipc://@omq_test_pipe_in_#{$$}")

      pipe_push = OMQ::PUSH.new.tap { |s| s.linger = 5 }
      pipe_push.connect("ipc://@omq_test_pipe_out_a_#{$$}")
      pipe_push.connect("ipc://@omq_test_pipe_out_b_#{$$}")

      wait_connected(source)
      sleep 0.001 until pipe_push.connection_count >= 2

      n       = 1000
      payload = "X" * 1024
      n.times { |i| source.send("#{i}:#{payload}") }

      # Pipe-style loop: receive one, send one, yield to let pumps drain
      n.times do
        parts = pipe_pull.receive
        pipe_push.send(parts)
        Async::Task.current.yield
      end

      source.close
      pipe_push.close

      counts  = { a: 0, b: 0 }
      total   = 0
      barrier = Async::Barrier.new
      [[:a, sink_a], [:b, sink_b]].each do |key, sink|
        barrier.async do
          loop do
            sink.receive
            counts[key] += 1
            barrier.stop if (total += 1) >= n
          end
        end
      end
      barrier.wait

      assert_equal n, counts[:a] + counts[:b], "all messages delivered"
      assert counts[:a] > 0, "expected sink_a to receive some messages, got #{counts[:a]}"
      assert counts[:b] > 0, "expected sink_b to receive some messages, got #{counts[:b]}"
    ensure
      pipe_pull&.close
      pipe_push&.close
      source&.close
      sink_a&.close
      sink_b&.close
    end
  end

end


describe "PUSH/PULL delivery guarantees" do
  before { OMQ::Transport::Inproc.reset! }

  # -- connect before bind (inproc) ----------------------------------------

  it "delivers messages when inproc connect happens before bind" do
    Async do
      push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.reconnect_interval = RECONNECT_INTERVAL
      push.connect("ruby://dg-inproc-cb")

      # Send while no peer is bound yet
      push.send("early-1")
      push.send("early-2")

      # Now bind
      pull = OMQ::PULL.bind("ruby://dg-inproc-cb")

      # Give reconnect a moment
      wait_connected(push, pull)

      push.send("late-1")

      msgs = []
      3.times do
        Async::Task.current.with_timeout(2) do
          msgs << pull.receive
        end
      end
      assert_equal [["early-1"], ["early-2"], ["late-1"]], msgs
    ensure
      push&.close
      pull&.close
    end
  end

  # -- bind before connect (inproc) ----------------------------------------

  it "delivers messages when inproc bind happens before connect" do
    Async do
      pull = OMQ::PULL.bind("ruby://dg-inproc-bc")
      push = OMQ::PUSH.connect("ruby://dg-inproc-bc")

      10.times { |i| push.send("msg-#{i}") }

      10.times do |i|
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        assert_equal ["msg-#{i}"], msg
      end
    ensure
      push&.close
      pull&.close
    end
  end

  # -- connect before bind (IPC) -------------------------------------------

  it "delivers messages when IPC connect happens before bind" do
    Async do
      path = "/tmp/omq-test-dg-ipc-cb-#{$$}.sock"
      push                      = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.reconnect_interval   = RECONNECT_INTERVAL
      push.connect("ipc://#{path}")

      push.send("early-1")

      sleep 0.02
      pull = OMQ::PULL.bind("ipc://#{path}")
      wait_connected(push, pull)

      push.send("late-1")

      msgs = []
      2.times do
        Async::Task.current.with_timeout(2) do
          msgs << pull.receive
        end
      end
      assert_equal [["early-1"], ["late-1"]], msgs
    ensure
      push&.close
      pull&.close
      File.delete(path) rescue nil
    end
  end

  # -- bind before connect (IPC) -------------------------------------------

  it "delivers messages when IPC bind happens before connect" do
    Async do
      path = "/tmp/omq-test-dg-ipc-bc-#{$$}.sock"
      pull = OMQ::PULL.bind("ipc://#{path}")

      push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.connect("ipc://#{path}")
      wait_connected(push, pull)

      5.times { |i| push.send("msg-#{i}") }

      5.times do |i|
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        assert_equal ["msg-#{i}"], msg
      end
    ensure
      push&.close
      pull&.close
      File.delete(path) rescue nil
    end
  end

  # -- connect before bind (TCP) -------------------------------------------

  it "delivers messages when TCP connect happens before bind" do
    Async do
      push                      = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.reconnect_interval   = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:19890")

      push.send("early-1")

      sleep 0.02
      pull = OMQ::PULL.bind("tcp://127.0.0.1:19890")
      wait_connected(push, pull)

      push.send("late-1")

      msgs = []
      2.times do
        Async::Task.current.with_timeout(2) do
          msgs << pull.receive
        end
      end
      assert_equal [["early-1"], ["late-1"]], msgs
    ensure
      push&.close
      pull&.close
    end
  end

  # -- bind before connect (TCP) -------------------------------------------

  it "delivers messages when TCP bind happens before connect" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      5.times { |i| push.send("msg-#{i}") }

      5.times do |i|
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        assert_equal ["msg-#{i}"], msg
      end
    ensure
      push&.close
      pull&.close
    end
  end

  # -- ordered delivery, no drops -------------------------------------------

  it "delivers all messages in order with no drops" do
    Async do
      pull = OMQ::PULL.bind("ruby://dg-order")
      push = OMQ::PUSH.connect("ruby://dg-order")

      n = 100
      n.times { |i| push.send("seq-#{i}") }

      received = []
      n.times do
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        received << msg.first
      end

      expected = n.times.map { |i| "seq-#{i}" }
      assert_equal expected, received
    ensure
      push&.close
      pull&.close
    end
  end

  # -- busy fiber during reconnect -----------------------------------------

  it "does not drop messages when receiver fiber is busy during TCP reconnect" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push                      = OMQ::PUSH.new.tap { |s| s.linger = 1 }
      push.reconnect_interval   = RECONNECT_INTERVAL
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      # Send first batch
      5.times { |i| push.send("batch1-#{i}") }

      # Simulate busy receiver — sleep before draining
      sleep 0.05

      # Receive first batch
      5.times do |i|
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        assert_equal ["batch1-#{i}"], msg
      end

      # Send second batch
      5.times { |i| push.send("batch2-#{i}") }

      5.times do |i|
        msg = Async::Task.current.with_timeout(2) do
          pull.receive
        end
        assert_equal ["batch2-#{i}"], msg
      end
    ensure
      push&.close
      pull&.close
    end
  end

  it "set_unbounded works (HWM=0)" do
    Async do
      push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.set_unbounded
      push.bind("ruby://pushpull-unbounded")

      pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
      pull.set_unbounded
      pull.connect("ruby://pushpull-unbounded")

      push.send("hello")
      msg = pull.receive
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end

  it "unbounded via HWM=nil" do
    Async do
      push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.send_hwm = nil
      push.recv_hwm = nil
      push.bind("ruby://pushpull-nil-hwm")

      pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
      pull.send_hwm = nil
      pull.recv_hwm = nil
      pull.connect("ruby://pushpull-nil-hwm")

      push.send("hello")
      msg = pull.receive
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end
end
