# frozen_string_literal: true

require_relative "../test_helper"

describe "send_timeout" do
  before { OMQ::Transport::Inproc.reset! }

  it "raises IO::TimeoutError when send blocks longer than send_timeout" do
    Async do
      # No peer connected — the send queue (HWM=1) fills immediately
      # and the second enqueue blocks until send_timeout fires.
      push = OMQ::PUSH.new(nil, send_hwm: 1, send_timeout: 0.02, linger: 0)
      push.bind("ipc://@omq-test-send-timeout")

      assert_raises IO::TimeoutError do
        2.times { push.send("fill") }
      end
    ensure
      push&.close
    end
  end

  it "does not raise when send completes within send_timeout" do
    Async do
      push = OMQ::PUSH.new(nil, send_timeout: 1)
      pull = OMQ::PULL.bind("ruby://timeout-send-ok")
      push.connect("ruby://timeout-send-ok")

      push.send("hello")
      msg = pull.receive
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end
end


describe "recv_timeout" do
  before { OMQ::Transport::Inproc.reset! }

  it "raises IO::TimeoutError when recv blocks longer than recv_timeout" do
    Async do
      pull = OMQ::PULL.new(nil, recv_timeout: 0.02)
      pull.bind("ruby://timeout-recv")

      push = OMQ::PUSH.connect("ruby://timeout-recv")

      # Don't send anything — recv should time out
      assert_raises IO::TimeoutError do
        pull.receive
      end
    ensure
      push&.close
      pull&.close
    end
  end

  it "does not raise when message arrives within recv_timeout" do
    Async do
      pull = OMQ::PULL.new(nil, recv_timeout: 2)
      pull.bind("ruby://timeout-recv-ok")

      push = OMQ::PUSH.connect("ruby://timeout-recv-ok")

      push.send("hello")
      msg = pull.receive
      assert_equal ["hello"], msg
    ensure
      push&.close
      pull&.close
    end
  end
end

describe "recv_timeout on other socket types" do
  before { OMQ::Transport::Inproc.reset! }

  it "works on SUB" do
    Async do
      pub = OMQ::PUB.bind("ruby://timeout-sub")
      sub = OMQ::SUB.connect("ruby://timeout-sub", subscribe: "")
      sub.recv_timeout = 0.02

      assert_raises(IO::TimeoutError) { sub.receive }
    ensure
      sub&.close
      pub&.close
    end
  end

  it "works on PAIR" do
    Async do
      a = OMQ::PAIR.bind("ruby://timeout-pair")
      b = OMQ::PAIR.connect("ruby://timeout-pair")
      b.recv_timeout = 0.02

      assert_raises(IO::TimeoutError) { b.receive }
    ensure
      a&.close
      b&.close
    end
  end

  it "works on REP" do
    Async do
      rep = OMQ::REP.bind("ruby://timeout-rep")
      rep.recv_timeout = 0.02

      assert_raises(IO::TimeoutError) { rep.receive }
    ensure
      rep&.close
    end
  end

  it "works on DEALER" do
    Async do
      router = OMQ::ROUTER.bind("ruby://timeout-dealer")
      dealer = OMQ::DEALER.connect("ruby://timeout-dealer")
      dealer.recv_timeout = 0.02

      assert_raises(IO::TimeoutError) { dealer.receive }
    ensure
      dealer&.close
      router&.close
    end
  end
end
