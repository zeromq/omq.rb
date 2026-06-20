# frozen_string_literal: true

require_relative "../test_helper"

describe "Error paths" do
  before { OMQ::Transport::Inproc.reset! }

  describe "bind to invalid transport" do
    it "raises ArgumentError" do
      push = OMQ::PUSH.new
      assert_raises(ArgumentError) do
        push.bind("nope://127.0.0.1:5555")
      end
    ensure
      push&.close
    end
  end

  describe "pump crash raises SocketDeadError" do
    it "surfaces on receive with the original error as cause" do
      Async do
        rep = OMQ::REP.bind("ruby://err-pump-recv")
        req = OMQ::REQ.connect("ruby://err-pump-recv")

        # Inject a crash into REQ's send pump
        conn = req.engine.connections.first.first
        def conn.write_message(_parts) = raise("boom")
        def conn.send_message(_parts)  = raise("boom")

        req << "trigger"
        sleep 0.01 # let the pump crash

        err = assert_raises(OMQ::SocketDeadError) { req << "again" }
        assert_match(/REQ/, err.message)
        assert_kind_of RuntimeError, err.cause
        assert_equal "boom", err.cause.message
      ensure
        req&.close
        rep&.close
      end
    end

    it "bricks the socket — all subsequent calls raise" do
      Async do
        rep = OMQ::REP.bind("ruby://err-pump-brick")
        req = OMQ::REQ.connect("ruby://err-pump-brick")

        conn = req.engine.connections.first.first
        def conn.write_message(_parts) = raise("boom")
        def conn.send_message(_parts)  = raise("boom")

        req << "trigger"
        sleep 0.01

        assert_raises(OMQ::SocketDeadError) { req << "one" }
        assert_raises(OMQ::SocketDeadError) { req << "two" }
      ensure
        req&.close
        rep&.close
      end
    end
  end

  describe "double close" do
    it "is idempotent on PUSH" do
      Async do
        push = OMQ::PUSH.new
        push.close
        push.close
      end
    end

    it "is idempotent on PULL" do
      Async do
        pull = OMQ::PULL.bind("ruby://err-dblclose")
        pull.close
        pull.close
      end
    end

    it "is idempotent on REP" do
      Async do
        rep = OMQ::REP.bind("ruby://err-dblclose-rep")
        rep.close
        rep.close
      end
    end

    it "is idempotent on PAIR" do
      Async do
        pair = OMQ::PAIR.bind("ruby://err-dblclose-pair")
        pair.close
        pair.close
      end
    end
  end
end
