# frozen_string_literal: true

require_relative "../test_helper"
require "weakref"

describe "inproc memory leaks" do
  before { OMQ::Transport::Inproc.reset! }

  # Collect until a WeakRef is dead, or give up after max attempts.
  #
  def gc_until_collected(weak, max: 20)
    max.times do
      return true unless weak.weakref_alive?
      GC.start(full_mark: true, immediate_sweep: true)
      GC.compact if GC.respond_to?(:compact)
    end
    !weak.weakref_alive?
  end

  it "does not leak Pipe objects after close" do
    weak = nil
    Async do
      push = OMQ::PUSH.new
      pull = OMQ::PULL.new
      push.bind("ruby://leak-test")
      pull.connect("ruby://leak-test")
      push << "hello"
      pull.receive

      # Track one of the pipes
      pipe = push.engine.connections.keys.first
      weak = WeakRef.new(pipe)
      pipe = nil

      push.close
      pull.close
    end

    assert gc_until_collected(weak), "Pipe was not collected after close"
  end


  it "does not leak connections after both sides close" do
    weak = nil
    Async do
      push = OMQ::PUSH.new
      pull = OMQ::PULL.new
      push.bind("ruby://leak-cycle")
      pull.connect("ruby://leak-cycle")
      push << "msg"
      pull.receive

      pipe = pull.engine.connections.first
      weak = WeakRef.new(pipe)
      pipe = nil

      push.close
      pull.close
    end

    assert gc_until_collected(weak), "Pipe was not collected after close"
  end


  it "cleans up the inproc registry after unbind" do
    Async do
      10.times do |i|
        ep = "ruby://leak-registry-#{i}"
        push = OMQ::PUSH.new
        push.bind(ep)
        push.close
      end

      registry = OMQ::Transport::Inproc.registry
      assert_equal 0, registry.size, "leaked #{registry.size} registry entries"
    end
  end


  it "does not grow the socket-level task barrier over many messages" do
    Async do
      push = OMQ::PUSH.new
      pull = OMQ::PULL.new
      push.bind("ruby://leak-tasks")
      pull.connect("ruby://leak-tasks")

      1000.times { |i| push << "msg-#{i}" }
      1000.times { pull.receive }

      push_barrier = push.engine.lifecycle.barrier
      pull_barrier = pull.engine.lifecycle.barrier

      # Tasks should be bounded — not one per message
      assert push_barrier.size < 10, "push barrier has #{push_barrier.size} tasks"
      assert pull_barrier.size < 10, "pull barrier has #{pull_barrier.size} tasks"
    ensure
      push&.close
      pull&.close
    end
  end
end
