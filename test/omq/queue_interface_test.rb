# frozen_string_literal: true

require_relative "../test_helper"

describe "QueueReadable" do
  before { OMQ::Transport::Inproc.reset! }

  it "#dequeue returns the next message" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-dequeue")
      push = OMQ::PUSH.connect("ruby://qi-dequeue")

      push.send("hello")
      assert_equal ["hello"], pull.dequeue
    ensure
      push&.close
      pull&.close
    end
  end

  it "#pop is an alias for #dequeue" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-pop")
      push = OMQ::PUSH.connect("ruby://qi-pop")

      push.send("hello")
      assert_equal ["hello"], pull.pop
    ensure
      push&.close
      pull&.close
    end
  end

  it "#wait blocks indefinitely, ignoring read_timeout" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-wait")
      push = OMQ::PUSH.connect("ruby://qi-wait")
      pull.read_timeout = 0.05

      Async do |task|
        sleep 0.1
        push.send("hello")
      end

      assert_equal ["hello"], pull.wait
    ensure
      push&.close
      pull&.close
    end
  end

  it "#dequeue accepts a timeout: kwarg" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-dequeue-timeout")

      assert_raises(IO::TimeoutError) { pull.dequeue(timeout: 0.05) }
    ensure
      pull&.close
    end
  end

  it "#each yields messages until socket is closed" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-each")
      push = OMQ::PUSH.connect("ruby://qi-each")

      push.send("a")
      push.send("b")
      push.send("c")

      received = []
      Async do
        pull.each do |msg|
          received << msg.first
          break if received.size == 3
        end
      end.wait

      assert_equal %w[a b c], received
    ensure
      push&.close
      pull&.close
    end
  end

  it "#each returns gracefully when read_timeout expires" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-each-timeout")
      push = OMQ::PUSH.connect("ruby://qi-each-timeout")
      pull.read_timeout = 0.05

      push.send("a")
      push.send("b")

      received = []
      pull.each { |msg| received << msg.first }

      assert_equal %w[a b], received
    ensure
      push&.close
      pull&.close
    end
  end
end


describe "QueueWritable" do
  before { OMQ::Transport::Inproc.reset! }

  it "#enqueue sends a message" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-enqueue")
      push = OMQ::PUSH.connect("ruby://qi-enqueue")

      push.enqueue("hello")
      assert_equal ["hello"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "#enqueue sends multiple messages" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-enqueue-multi")
      push = OMQ::PUSH.connect("ruby://qi-enqueue-multi")

      push.enqueue("a", "b", "c")
      assert_equal ["a"], pull.receive
      assert_equal ["b"], pull.receive
      assert_equal ["c"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "#push is an alias for #enqueue" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-push")
      push = OMQ::PUSH.connect("ruby://qi-push")

      push.push("hello")
      assert_equal ["hello"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "#enqueue returns self for chaining" do
    Async do
      pull = OMQ::PULL.bind("ruby://qi-chain")
      push = OMQ::PUSH.connect("ruby://qi-chain")

      assert_equal push, push.enqueue("hello")
    ensure
      push&.close
      pull&.close
    end
  end
end
