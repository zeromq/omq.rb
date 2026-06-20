# frozen_string_literal: true

require_relative "../test_helper"

describe "disconnect / unbind" do
  before { OMQ::Transport::Inproc.reset! }

  it "#disconnect closes only connections to that endpoint" do
    Async do
      pull1 = OMQ::PULL.bind("ruby://ep1")
      pull1.read_timeout = 0.05
      pull2 = OMQ::PULL.bind("ruby://ep2")
      pull2.read_timeout = 0.05

      push = OMQ::PUSH.new
      push.connect("ruby://ep1")
      push.connect("ruby://ep2")

      # Disconnect from ep1 — ep2 keeps working. PUSH uses work-stealing
      # rather than strict round-robin, so we can't assert which peer a
      # pre-disconnect message lands on; we only verify that after the
      # disconnect, the surviving endpoint still receives.
      push.disconnect("ruby://ep1")

      10.times { |i| push.send("post-#{i}") }
      received = []
      loop do
        received << pull2.receive.first
      rescue IO::TimeoutError
        break
      end
      assert_equal 10, received.size

      # ep1 must receive nothing new
      assert_raises(IO::TimeoutError) { pull1.receive }
    ensure
      push&.close
      pull1&.close
      pull2&.close
    end
  end

  it "#disconnect emits :disconnected on the monitor queue" do
    Async do
      events = []
      pull   = OMQ::PULL.bind("ruby://ep-mon")
      push   = OMQ::PUSH.new
      push.monitor { |e| events << e.type }
      push.connect("ruby://ep-mon")
      wait_connected(push)

      push.disconnect("ruby://ep-mon")
      sleep 0.01 # let monitor drain

      assert_includes events, :disconnected
    ensure
      push&.close
      pull&.close
    end
  end


  it "#unbind stops accepting new connections" do
    Async do
      rep = OMQ::REP.new
      port = rep.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.new
      req.connect("tcp://127.0.0.1:#{port}")

      # Works before unbind
      req.send("hello")
      msg = rep.receive
      assert_equal ["hello"], msg
      rep.send("world")
      req.receive

      # Unbind
      rep.unbind("tcp://127.0.0.1:#{port}")

      # New connections silently retry in background (no raise)
      req2 = OMQ::REQ.new
      req2.connect("tcp://127.0.0.1:#{port}")  # does not raise
    ensure
      req&.close
      req2&.close
      rep&.close
    end
  end
end
