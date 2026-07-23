# frozen_string_literal: true

require_relative "../test_helper"

describe "libzmq backend" do
  before { skip "libzmq not available" unless OMQ_FFI_AVAILABLE }

  it "PUSH/PULL over TCP" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port
      refute_nil port
      assert port > 0

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send("hello libzmq")
      msg = pull.receive
      assert_equal ["hello libzmq"], msg
    ensure
      push&.close
      pull&.close
    end
  end

  it "REQ/REP over TCP" do
    Async do
      rep = OMQ::REP.new(backend: :libzmq)
      port = rep.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.new(backend: :libzmq)
      req.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      req.send("request")
      assert_equal ["request"], rep.receive

      rep.send("reply")
      assert_equal ["reply"], req.receive
    ensure
      req&.close
      rep&.close
    end
  end

  it "PAIR over TCP" do
    Async do
      server = OMQ::PAIR.new(backend: :libzmq)
      port = server.bind("tcp://127.0.0.1:0").port

      client = OMQ::PAIR.new(backend: :libzmq)
      client.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      client.send("hello")
      assert_equal ["hello"], server.receive

      server.send("world")
      assert_equal ["world"], client.receive
    ensure
      client&.close
      server&.close
    end
  end

  it "PUB/SUB with topic filtering" do
    Async do
      pub = OMQ::PUB.new(backend: :libzmq)
      port = pub.bind("tcp://127.0.0.1:0").port

      sub = OMQ::SUB.new(subscribe: "weather.", backend: :libzmq)
      sub.connect("tcp://127.0.0.1:#{port}")
      sleep 0.03

      pub.send("weather.rain")
      pub.send("sports.goal")   # should be filtered out
      pub.send("weather.sun")

      assert_equal ["weather.rain"], sub.receive
      assert_equal ["weather.sun"], sub.receive
    ensure
      sub&.close
      pub&.close
    end
  end

  it "PUB/SUB subscribe to everything" do
    Async do
      pub = OMQ::PUB.new(backend: :libzmq)
      port = pub.bind("tcp://127.0.0.1:0").port

      sub = OMQ::SUB.new(subscribe: "", backend: :libzmq)
      sub.connect("tcp://127.0.0.1:#{port}")
      sleep 0.03

      pub.send("any topic")
      assert_equal ["any topic"], sub.receive
    ensure
      sub&.close
      pub&.close
    end
  end

  it "multipart messages" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send(["part1", "part2", "part3"])
      assert_equal ["part1", "part2", "part3"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "empty frame in multipart" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      push.send(["identity", "", "body"])
      msg = pull.receive
      assert_equal 3, msg.size
      assert_equal "identity", msg[0]
      assert_equal "", msg[1]
      assert_equal "body", msg[2]
    ensure
      push&.close
      pull&.close
    end
  end

  it "binary data preserved" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      binary = (0..255).map(&:chr).join.b
      push.send(binary)
      msg = pull.receive
      assert_equal [binary], msg
    ensure
      push&.close
      pull&.close
    end
  end

  it "large messages (64 KB)" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      big = "X" * 65_536
      push.send(big)
      msg = pull.receive
      assert_equal [big], msg
      assert_equal 65_536, msg.first.bytesize
    ensure
      push&.close
      pull&.close
    end
  end

  it "many messages without deadlock" do
    Async do
      pull = OMQ::PULL.new(backend: :libzmq)
      port = pull.bind("tcp://127.0.0.1:0").port

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      n = 5000
      100.times do
        push << "warmup"
        pull.receive
      end

      sender = Async { n.times { |i| push << "msg-#{i}" } }
      n.times { pull.receive }
      sender.wait
    ensure
      push&.close
      pull&.close
    end
  end

  it "over IPC" do
    Async do
      addr = "ipc:///tmp/omq_ffi_test_#{$$}.sock"

      pull = OMQ::PULL.new(backend: :libzmq)
      pull.bind(addr)

      push = OMQ::PUSH.new(backend: :libzmq)
      push.connect(addr)
      sleep 0.01

      push.send("ipc msg")
      assert_equal ["ipc msg"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

  it "DEALER/ROUTER with identity" do
    Async do
      router = OMQ::ROUTER.new(backend: :libzmq)
      port = router.bind("tcp://127.0.0.1:0").port

      dealer = OMQ::DEALER.new(backend: :libzmq)
      dealer.identity = "worker-1"
      dealer.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      dealer.send("hello from dealer")
      msg = router.receive
      # ROUTER prepends the sender identity frame
      assert_equal "worker-1", msg[0]
      assert msg.size >= 2
    ensure
      dealer&.close
      router&.close
    end
  end

  it "multiple REQ/REP roundtrips" do
    Async do
      rep = OMQ::REP.new(backend: :libzmq)
      port = rep.bind("tcp://127.0.0.1:0").port

      req = OMQ::REQ.new(backend: :libzmq)
      req.connect("tcp://127.0.0.1:#{port}")
      sleep 0.01

      20.times do |i|
        req.send("request-#{i}")
        assert_equal ["request-#{i}"], rep.receive
        rep.send("reply-#{i}")
        assert_equal ["reply-#{i}"], req.receive
      end
    ensure
      req&.close
      rep&.close
    end
  end

  it "backend: :ruby is default (nil)" do
    Async do
      push_default = OMQ::PUSH.new
      push_ruby    = OMQ::PUSH.new(backend: :ruby)
      # Both should use the native Ruby engine
      assert_instance_of OMQ::Engine, push_default.engine
      assert_instance_of OMQ::Engine, push_ruby.engine
    ensure
      push_default&.close
      push_ruby&.close
    end
  end

  it "backend: :ffi aliases libzmq engine" do
    Async do
      require "omq/ffi"
      push = OMQ::PUSH.new(backend: :ffi)
      assert_instance_of OMQ::FFI::Engine, push.engine
    ensure
      push&.close
    end
  end

  it "backend: :libzmq uses libzmq engine" do
    Async do
      push = OMQ::PUSH.new(backend: :libzmq)
      assert_instance_of OMQ::FFI::Engine, push.engine
    ensure
      push&.close
    end
  end

  it "raises on unknown backend" do
    assert_raises(ArgumentError) { OMQ::PUSH.new(backend: :bogus) }
  end

  it "bricks the socket when the I/O thread dies" do
    Async do
      push = OMQ::PUSH.new(backend: :libzmq)
      engine = push.engine
      def engine.drain_sends = raise("ffi boom")

      push << "trigger"

      err = nil
      deadline = Async::Clock.now + 1.0
      until err
        begin
          push << "again"
        rescue OMQ::SocketDeadError => e
          err = e
        end

        raise "socket did not brick" if Async::Clock.now >= deadline

        sleep 0.01
      end

      assert_match(/PUSH/, err.message)
      assert_kind_of RuntimeError, err.cause
      assert_equal "ffi boom", err.cause.message
      assert_raises(OMQ::SocketDeadError) { push << "third" }
    ensure
      push&.close
    end
  end

  it "wakes a waiting command when command processing dies" do
    Async do
      push = OMQ::PUSH.new(backend: :libzmq)
      push.identity = "peer"
      engine = push.engine
      def engine.set_bytes_option(*) = raise("cmd boom")

      err = assert_raises(OMQ::SocketDeadError) do
        push.bind("tcp://127.0.0.1:0")
      end

      assert_match(/PUSH/, err.message)
      assert_kind_of RuntimeError, err.cause
      assert_equal "cmd boom", err.cause.message
      assert_raises(OMQ::SocketDeadError) { push << "again" }
    ensure
      push&.close
    end
  end

  describe "errno mapping" do
    it "bind to a busy TCP port raises Errno::EADDRINUSE (same as pure Ruby)" do
      Async do
        holder = OMQ::PULL.new(backend: :libzmq)
        port = holder.bind("tcp://127.0.0.1:0").port

        clash = OMQ::PULL.new(backend: :libzmq)
        error = assert_raises(Errno::EADDRINUSE) { clash.bind("tcp://127.0.0.1:#{port}") }
        assert_kind_of SystemCallError, error
      ensure
        clash&.close
        holder&.close
      end
    end

    it "connect to a bogus endpoint raises a SystemCallError (not RuntimeError)" do
      Async do
        push = OMQ::PUSH.new(backend: :libzmq)
        error = assert_raises(SystemCallError) { push.connect("tcp://not a valid endpoint") }
        refute_equal SystemCallError, error.class  # expect a specific Errno::X subclass
      ensure
        push&.close
      end
    end
  end
end
