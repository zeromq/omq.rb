# frozen_string_literal: true

require_relative "../test_helper"

describe "libzmq ↔ pure Ruby interop" do
  before { skip "libzmq not available" unless OMQ_FFI_AVAILABLE }

  it "libzmq PUSH → pure Ruby PULL" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send("cross-backend")
      assert_equal ["cross-backend"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "libzmq PUSH drains send queue when closed immediately after send" do
    # Regression: io_loop used to break on :stop before draining @send_queue,
    # so send-then-close would lose the message instead of letting LINGER flush it.
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq, linger: 5)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send("flushed-on-close")
      push.close  # close immediately, no yield in between

      assert_equal ["flushed-on-close"], pull.receive
    ensure
      pull&.close
    end
  end


  it "pure Ruby PUSH → libzmq PULL" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      push.send("cross-backend")
      assert_equal ["cross-backend"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "libzmq REQ ↔ pure Ruby REP roundtrip" do
    Async do
      rep = OMQ::REP.new
      port = rep.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.new(backend: :libzmq)
      req.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      req.send("ping")
      assert_equal ["ping"], rep.receive

      rep.send("pong")
      assert_equal ["pong"], req.receive
    ensure
      req&.close
      rep&.close
    end
  end

  it "pure Ruby REQ ↔ libzmq REP roundtrip" do
    Async do
      rep = OMQ::REP.new(backend: :libzmq)
      port = rep.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.connect("tcp://127.0.0.1:#{port}")
      wait_connected(req)

      req.send("ping")
      assert_equal ["ping"], rep.receive

      rep.send("pong")
      assert_equal ["pong"], req.receive
    ensure
      req&.close
      rep&.close
    end
  end

  it "multipart interop (libzmq -> pure Ruby)" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send(["id", "", "payload"])
      assert_equal ["id", "", "payload"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "multipart interop (pure Ruby -> libzmq)" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")
      wait_connected(push)

      push.send(["id", "", "payload"])
      assert_equal ["id", "", "payload"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "PUB/SUB interop (libzmq PUB → pure Ruby SUB)" do
    Async do
      pub = OMQ::PUB.new(backend: :libzmq)
      port = pub.bind("tcp://127.0.0.1:0").port

      sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "")
      wait_connected(sub)
      sleep 0.05  # subscription propagation

      pub.send("cross-pubsub")
      assert_equal ["cross-pubsub"], sub.receive
    ensure
      sub&.close
      pub&.close
    end
  end
end

