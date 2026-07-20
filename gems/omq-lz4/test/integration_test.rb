# frozen_string_literal: true

require_relative "test_helper"
require "async"
require "omq"
require "omq/lz4"

describe "lz4+tcp:// transport" do
  it "round-trips a small payload below the compression threshold" do
    Sync do
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0")
      push.connect(uri.to_s)

      push << ["hi"]
      assert_equal ["hi"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "round-trips a payload large enough to compress" do
    Sync do
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0")
      push.connect(uri.to_s)

      payload = ("A" * 4096).b
      push << [payload]
      assert_equal [payload], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "round-trips a multipart message" do
    Sync do
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0")
      push.connect(uri.to_s)

      parts = ["header", "body " * 300, "trailer"]
      push << parts
      assert_equal parts, pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "ships a configured sender-side dict once and round-trips subsequent messages" do
    dict = ("event=login user=alice payload=" * 10).b

    Sync do
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0")
      push.connect(uri.to_s, dict: dict)

      msg1 = ("event=login user=alice payload=first").b
      msg2 = ("event=login user=alice payload=second").b

      push << [msg1]
      push << [msg2]

      assert_equal [msg1], pull.receive
      assert_equal [msg2], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "dict shipment on connect: receiver also configured with dict works" do
    # Both sides have the same dict on their own send side, so each
    # direction ships its own dict. PUSH→PULL only uses the push side's
    # dict; this test just verifies the multi-direction case doesn't
    # break anything.
    dict = ("common prefix " * 8).b

    Sync do
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0", dict: dict)
      push.connect(uri.to_s, dict: dict)

      msg = (dict + "body").b
      push << [msg]
      assert_equal [msg], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  describe "auto_dict training (RFC §7.6)" do
    # Structured messages that give the COVER trainer enough signal.
    def json_msg(i)
      %Q({"event":"login","user":"user_#{i}","ts":"2026-06-12T00:00:00.#{format("%04d", i)}Z","region":"us-east-1","status":200}).b
    end

    it "auto-trains from early traffic and round-trips subsequent messages" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        push.connect(uri.to_s, auto_dict: { capacity: 2048, trigger: 20 })

        # Send 20 messages to trigger training (trigger=20).
        20.times { |i| push << [json_msg(i)] }
        20.times { |i| assert_equal [json_msg(i)], pull.receive }

        # Subsequent messages use the trained dict and round-trip.
        10.times do |i|
          msg = json_msg(1000 + i)
          push << [msg]
          assert_equal [msg], pull.receive
        end
      ensure
        push&.close
        pull&.close
      end
    end

    it "auto-trains with default trigger (100 messages)" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        push.connect(uri.to_s, auto_dict: true)

        110.times { |i| push << [json_msg(i)] }
        110.times { |i| assert_equal [json_msg(i)], pull.receive }
      ensure
        push&.close
        pull&.close
      end
    end

    it "auto-trains on the bind side" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0", auto_dict: { trigger: 20 })
        push.connect(uri.to_s)

        # PUSH->PULL is one-directional, so the bind-side auto_dict
        # affects the pull's send side (unused here). Verify the
        # connection still works.
        25.times { |i| push << [json_msg(i)] }
        25.times { |i| assert_equal [json_msg(i)], pull.receive }
      ensure
        push&.close
        pull&.close
      end
    end

    it "stays in no-dict mode when training fails (too few distinct samples)" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        # trigger=5, but only 2-byte messages which DictTrainer skips
        # (< MINMATCH=4). Training produces empty dict -> no-dict mode.
        push.connect(uri.to_s, auto_dict: { trigger: 5 })

        8.times { push << ["hi"] }
        8.times { assert_equal ["hi"], pull.receive }
      ensure
        push&.close
        pull&.close
      end
    end

    it "rejects auto_dict combined with dict" do
      Sync do
        pull = OMQ::PULL.new
        assert_raises(ArgumentError) do
          pull.bind("lz4+tcp://127.0.0.1:0", dict: "some dict bytes!", auto_dict: true)
        end
      ensure
        pull&.close
      end
    end

    it "rejects auto_dict with capacity exceeding MAX_DICT_SIZE" do
      Sync do
        pull = OMQ::PULL.new
        assert_raises(ArgumentError) do
          pull.bind("lz4+tcp://127.0.0.1:0", auto_dict: { capacity: 16_384 })
        end
      ensure
        pull&.close
      end
    end

    it "rejects auto_dict with capacity 0" do
      Sync do
        pull = OMQ::PULL.new
        assert_raises(ArgumentError) do
          pull.bind("lz4+tcp://127.0.0.1:0", auto_dict: { capacity: 0 })
        end
      ensure
        pull&.close
      end
    end
  end


  it "rejects a bind with an oversized dict" do
    oversized = ("x" * (OMQ::LZ4::Codec::MAX_DICT_SIZE + 1)).b
    Sync do
      pull = OMQ::PULL.new
      assert_raises(OMQ::LZ4::ProtocolError) do
        pull.bind("lz4+tcp://127.0.0.1:0", dict: oversized)
      end
    ensure
      pull&.close
    end
  end


  describe "receiver size budget (max_message_size)" do
    it "rejects a single-part message whose decompressed size exceeds max_message_size" do
      Sync do
        pull = OMQ::PULL.new
        pull.max_message_size = 1024

        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        # 10 KiB of 'A's compresses to ~40 bytes over the wire, but
        # declared decompressed size in the LZ4B header is 10 240 —
        # far over the 1 KiB budget. Must be caught BEFORE decoder
        # invocation: OMQ::LZ4::ProtocolError propagates and closes
        # the connection; pull.receive then raises SocketDeadError.
        push << [("A" * 10_240).b]

        assert_raises(OMQ::SocketDeadError) { pull.receive }
      ensure
        push&.close
        pull&.close
      end
    end


    it "rejects a multipart message whose combined decompressed size exceeds max_message_size" do
      Sync do
        pull = OMQ::PULL.new
        pull.max_message_size = 2000

        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        # Three 1 KiB parts individually under budget; sum (3072) over.
        push << [("A" * 1024).b, ("B" * 1024).b, ("C" * 1024).b]

        assert_raises(OMQ::SocketDeadError) { pull.receive }
      ensure
        push&.close
        pull&.close
      end
    end


    it "accepts a multipart message whose combined decompressed size is under max_message_size" do
      Sync do
        pull = OMQ::PULL.new
        pull.max_message_size = 4096

        push = OMQ::PUSH.new
        uri  = pull.bind("lz4+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        parts = [("A" * 1024).b, ("B" * 1024).b, ("C" * 1024).b]
        push << parts

        assert_equal parts, pull.receive
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "dictionary shipment state" do
    it "raises on a second LZ4D shipment on the same direction" do
      # The transport's outgoing path only ships its dict once (guarded
      # by @send_dict_shipped), so we can't provoke a second shipment
      # through a well-behaved peer. This test exercises the receiver
      # rule directly by driving install_recv_dict! on an Lz4Connection.
      conn = OMQ::Transport::Lz4Tcp::Lz4Connection.new(
        Object.new,
        send_dict_bytes:  nil,
        max_message_size: nil,
      )

      first  = OMQ::LZ4::Codec.encode_dict_shipment("dict A")
      second = OMQ::LZ4::Codec.encode_dict_shipment("dict B")

      conn.send(:install_recv_dict!, first)
      err = assert_raises(OMQ::LZ4::ProtocolError) do
        conn.send(:install_recv_dict!, second)
      end
      assert_match(/second dictionary shipment/i, err.message)
    end
  end


  it "exchanges 100k messages without leaking memory" do
    skip "set OMQ_LZ4_STRESS=1 to run the 100k message soak" unless ENV["OMQ_LZ4_STRESS"]

    n = 100_000

    GC.start
    before_live = GC.stat(:heap_live_slots)

    Sync do |task|
      pull = OMQ::PULL.new
      push = OMQ::PUSH.new
      uri  = pull.bind("lz4+tcp://127.0.0.1:0")
      push.connect(uri.to_s)

      msg = ("payload " * 50).b

      # Sender and receiver must run concurrently: sequential send-all then
      # receive-all deadlocks once the TCP send buffer fills and the suspended
      # sender fiber has no peer to drain it.
      sender   = task.async { n.times { push << [msg] } }
      receiver = task.async { n.times { assert_equal [msg], pull.receive } }
      sender.wait
      receiver.wait
    ensure
      push&.close
      pull&.close
    end

    GC.start
    after_live = GC.stat(:heap_live_slots)

    # Leak check: live-slot delta after full GC, across a round-trip of
    # 100k messages, should be small and unrelated to N. A genuine
    # per-connection/per-message leak would grow linearly with N; a
    # ~constant residue from test harness + sockets + Async state is
    # expected. Cap generously at 20k slots — catches a leak of even
    # 0.2 slots/message.
    leaked = after_live - before_live
    puts "live-slot delta after 100k messages: #{leaked}"
    assert_operator leaked, :<, 20_000,
      "live-slot delta #{leaked} suggests a per-message leak"
  end
end
