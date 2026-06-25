# frozen_string_literal: true

require_relative "../test_helper"

describe "Socket#monitor" do
  before { OMQ::Transport::Inproc.reset! }

  # -- TCP bind side ---------------------------------------------------------

  it "emits lifecycle events in order for TCP bind side" do
    Async do
      events = []
      pull   = OMQ::PULL.new.tap { |s| s.linger = 0 }
      pull.monitor { |e| events << e }

      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      push.send("hello")
      msg = Async::Task.current.with_timeout(2) { pull.receive }
      assert_equal ["hello"], msg

      push.close
      sleep 0.02 # let disconnected propagate
      pull.close
      sleep 0.01 # let monitor drain

      types = events.map(&:type)
      assert_equal :listening,            types[0]
      assert_equal :accepted,             types[1]
      assert_equal :handshake_succeeded,  types[2]
      assert_equal :disconnected,         types[3]
      assert_equal :closed,               types[4]
      assert_equal :monitor_stopped,      types[5]
    end
  end

  # -- TCP connect side ------------------------------------------------------

  it "emits lifecycle events in order for TCP connect side" do
    Async do
      events = []
      push   = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.monitor { |e| events << e }

      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      pull.close
      sleep 0.02 # let disconnected propagate
      push.close
      sleep 0.01 # let monitor drain

      # Filter retry events: after :disconnected, the reconnect loop may
      # fire :connect_retried before push.close runs. Orthogonal to lifecycle.
      types = events.map(&:type).reject { |t| t == :connect_retried }
      assert_equal :connect_delayed,      types[0]
      assert_equal :connected,            types[1]
      assert_equal :handshake_succeeded,  types[2]
      assert_equal :disconnected,         types[3]
      assert_equal :closed,               types[4]
      assert_equal :monitor_stopped,      types[5]
    end
  end

  # -- Inproc lifecycle ------------------------------------------------------

  it "emits lifecycle events in order for inproc" do
    Async do
      events = []
      pull   = OMQ::PULL.new.tap { |s| s.linger = 0 }
      pull.monitor { |e| events << e }

      pull.bind("ruby://monitor-inproc")
      push = OMQ::PUSH.connect("ruby://monitor-inproc")

      push.send("hello")
      msg = Async::Task.current.with_timeout(2) { pull.receive }
      assert_equal ["hello"], msg

      push.close
      sleep 0.01
      pull.close
      sleep 0.01

      types = events.map(&:type)
      assert_equal :listening,            types[0]
      assert_equal :handshake_succeeded,  types[1]
      assert_equal :closed,               types[-2]
      assert_equal :monitor_stopped,      types[-1]
    end
  end

  # -- Disconnect reason -----------------------------------------------------

  it "includes the ZMTP error message in :disconnected detail" do
    Async do
      events = []
      pull   = OMQ::PULL.new.tap { |s| s.linger = 0 }
      pull.max_message_size = 10
      pull.monitor { |e| events << e }

      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      push.send("x" * 100)
      sleep 0.05 # let disconnected propagate

      disconnected = events.find { |e| e.type == :disconnected }
      refute_nil disconnected, "expected a :disconnected event"
      refute_nil disconnected.detail, "expected :disconnected to carry detail"

      error = disconnected.detail[:error]
      assert_kind_of Protocol::ZMTP::Error, error, "expected detail[:error] to be a Protocol::ZMTP::Error"

      reason = disconnected.detail[:reason]
      refute_nil reason, "expected :disconnected detail to carry a reason"
      assert_match(/max_message_size|message size|too (large|big)/i, reason)
    ensure
      push&.close
      pull&.close
    end
  end

  # -- Reconnect events ------------------------------------------------------

  it "emits connect_delayed then connect_retried on failed connects" do
    Async do
      events = []
      push   = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.reconnect_interval = RECONNECT_INTERVAL
      push.monitor { |e| events << e }

      push.connect("tcp://127.0.0.1:19891")
      sleep 0.1 # let it fail a few times

      types = events.map(&:type)
      assert_equal :connect_delayed, types[0]
      assert types[1..].all? { |t| t == :connect_retried },
        "all events after connect_delayed should be connect_retried, got: #{types[1..]}"

      retried = events.find { |e| e.type == :connect_retried }
      assert retried.detail[:interval], "connect_retried should include interval"

      push.close
    end
  end

  # -- Disconnected event ----------------------------------------------------

  it "emits disconnected when peer closes" do
    Async do
      events = []
      push   = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      push.reconnect_enabled = false
      push.monitor { |e| events << e }

      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push, pull)

      pull.close
      sleep 0.05 # let disconnected propagate

      types = events.map(&:type)
      assert_equal :connect_delayed,      types[0]
      assert_equal :connected,            types[1]
      assert_equal :handshake_succeeded,  types[2]
      assert_equal :disconnected,         types[3]

      push.close
    end
  end

  # -- Early stop ------------------------------------------------------------

  it "emits monitor_stopped when task is stopped early" do
    Async do
      events = []
      push   = OMQ::PUSH.new.tap { |s| s.linger = 0 }
      task   = push.monitor { |e| events << e }

      push.bind("tcp://127.0.0.1:0")
      sleep 0.01

      task.stop
      sleep 0.01

      assert_equal :listening,       events.first.type
      assert_equal :monitor_stopped, events.last.type

      push.close
    end
  end

  # -- MonitorEvent is pattern-matchable ------------------------------------

  it "supports pattern matching" do
    event = OMQ::MonitorEvent.new(type: :connected, endpoint: "tcp://127.0.0.1:5555")

    matched = case event
              in type: :connected, endpoint:
                endpoint
              end

    assert_equal "tcp://127.0.0.1:5555", matched
  end

  # -- No overhead without monitor -------------------------------------------

  it "works normally without monitor attached" do
    Async do
      pull = OMQ::PULL.bind("ruby://monitor-none")
      push = OMQ::PUSH.connect("ruby://monitor-none")

      push.send("hello")
      msg = Async::Task.current.with_timeout(2) { pull.receive }
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end
end
