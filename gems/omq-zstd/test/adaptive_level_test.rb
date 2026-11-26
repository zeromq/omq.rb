# frozen_string_literal: true

require_relative "test_helper"
require "async"
require "omq"
require "omq/radio_dish"

class OMQZstdAdaptiveLevelTest < Minitest::Spec
  ZstdTcp = OMQ::Transport::ZstdTcp


  describe ZstdTcp::Bottleneck do
    # Ported from omq.rs omq-proto/src/proto/transform/adaptive/tests.rs.
    def window(blocked_ms, backlogged, overflowed, input_full, batches: 64, elapsed: 0.1)
      ZstdTcp::Bottleneck.classify(
        elapsed:    elapsed,
        blocked:    blocked_ms / 1000.0,
        batches:    batches,
        backlogged: backlogged,
        overflowed: overflowed,
        input_full: input_full,
      )
    end


    it "names the bottleneck of clear windows" do
      assert_equal :output, window(100, 64, 64, 1)
      assert_equal :output, window(7, 0, 0, 0)
      assert_equal :cpu,    window(0, 64, 0, 1)
      assert_equal :cpu,    window(0, 64, 9, 0)
      assert_equal :input,  window(0, 0, 0, 0)
    end


    it "names none for mixed and empty windows" do
      assert_nil window(0, 64, 0, 0)
      assert_nil window(0, 64, 8, 0)
      assert_nil window(3, 64, 64, 1)
      assert_nil window(0, 0, 0, 0, batches: 0)
      assert_nil window(0, 0, 0, 0, elapsed: 0)
    end
  end


  describe ZstdTcp::AdaptiveSender do
    # `queued` is whether the socket still has messages to send after each
    # batch.
    def sender(queued: true)
      codec = ZstdTcp::Codec.new(level: ZstdTcp::START_LEVEL, level_range: ZstdTcp::DEFAULT_LEVEL_RANGE)
      ZstdTcp::AdaptiveSender.new(codec, send_hwm: 1000, queued: -> { queued })
    end


    # Compresses a window's worth of input, as `batches` batches of 256
    # messages (each capped), then closes the window at `elapsed`.
    def run_window(sender, blocked:, capped:, elapsed: 0.2)
      started = sender.window_started
      message = [Random.bytes(1024)]
      20.times { sender.codec.compress_parts([Random.bytes(16 * 1024)], sender.frame_codec) }
      8.times do
        sender.note_batch(capped ? Array.new(256, message) : [message])
      end
      sender.note_write(blocked)
      sender.maybe_adapt(started + elapsed)
    end


    it "starts at -1 and steps toward speed when the CPU is the limit" do
      s = sender
      assert_equal(-1, s.frame_codec.level)
      run_window(s, blocked: 0.0, capped: true)
      assert_operator s.frame_codec.level, :<, -1
    end


    it "steps toward ratio when writes stall" do
      s = sender
      run_window(s, blocked: 0.05, capped: false)
      assert_operator s.frame_codec.level, :>, -1
    end


    it "counts only the time a write takes beyond 1 ms as stalled" do
      s = sender
      200.times { s.note_write(0.0005) }
      run_window(s, blocked: 0.0, capped: true)
      assert_operator s.frame_codec.level, :<, -1
    end


    it "steps toward ratio when capped batches leave nothing queued" do
      s = sender(queued: false)
      run_window(s, blocked: 0.0, capped: true)
      assert_operator s.frame_codec.level, :>, -1
    end


    it "waits for 100 ms and 256 KiB of input before it adapts" do
      s = sender
      run_window(s, blocked: 0.0, capped: true, elapsed: 0.05)
      assert_equal(-1, s.frame_codec.level)
    end
  end


  describe "queued_probe" do
    it "says whether the socket has messages queued to send" do
      Sync do
        push  = OMQ::PUSH.new(linger: 0)
        probe = ZstdTcp.queued_probe(push.instance_variable_get(:@engine))
        refute probe.call
        push << ["no peer, so it stays queued"]
        assert probe.call
      ensure
        push&.close
      end
    end
  end


  describe "level: option" do
    def connections(socket)
      socket.instance_variable_get(:@engine).connections.keys
    end


    def zstd_connection(push, pull)
      push << ["hello " * 200]
      pull.receive
      conn = connections(push).first
      conn = conn.__getobj__ until conn.is_a?(ZstdTcp::ZstdConnection) || !conn.respond_to?(:__getobj__)
      conn
    end


    it "adapts by default, keeps an Integer level fixed and adapts within a Range" do
      Sync do
        results = { nil => nil, -3 => nil, (-5..2) => nil }.to_h do |level, _|
          pull = OMQ::PULL.new
          push = OMQ::PUSH.new(linger: 0)
          uri  = pull.bind("zstd+tcp://127.0.0.1:0")
          level.nil? ? push.connect(uri.to_s) : push.connect(uri.to_s, level: level)
          conn = zstd_connection(push, pull)
          push.close
          pull.close
          [level, conn.instance_variable_get(:@sender)]
        end
        refute_nil results[nil]
        assert_nil results[-3]
        refute_nil results[-5..2]
      end
    end


    it "keeps fan-out at the adaptive start level" do
      Sync do
        sub = OMQ::SUB.new
        sub.subscribe("")
        pub = OMQ::PUB.new(linger: 0)
        uri = sub.bind("zstd+tcp://127.0.0.1:0")
        pub.connect(uri.to_s)
        codec = ZstdTcp.instance_variable_get(:@codecs)[pub.instance_variable_get(:@engine)]
        assert_nil codec.level_range
        assert_equal ZstdTcp::START_LEVEL, codec.level
      ensure
        pub&.close
        sub&.close
      end
    end


    it "keeps RADIO at the adaptive start level" do
      Sync do
        dish  = OMQ::DISH.new
        radio = OMQ::RADIO.new(linger: 0)
        uri   = dish.bind("zstd+tcp://127.0.0.1:0")
        radio.connect(uri.to_s)
        codec = ZstdTcp.instance_variable_get(:@codecs)[radio.instance_variable_get(:@engine)]
        assert_nil codec.level_range
        assert_equal ZstdTcp::START_LEVEL, codec.level
      ensure
        radio&.close
        dish&.close
      end
    end


    it "rejects a level zrip lacks and unknown options" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new(linger: 0)
        uri  = pull.bind("zstd+tcp://127.0.0.1:0")
        assert_raises(ArgumentError) { push.connect(uri.to_s, level: 9) }
        assert_raises(ArgumentError) { push.connect(uri.to_s, level: 10..20) }
        assert_raises(ArgumentError) { push.connect(uri.to_s, levle: 3) }
        assert_raises(ArgumentError) { pull.bind("zstd+tcp://127.0.0.1:0", profile: :ratio) }
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
