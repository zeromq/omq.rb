# frozen_string_literal: true

require_relative "../test_helper"
require "tmpdir"
require "securerandom"

describe "received parts are frozen" do
  before { OMQ::Transport::Inproc.reset! }


  def assert_frozen_message(msg)
    assert msg.is_a?(Array), "expected Array, got #{msg.class}"
    assert msg.frozen?, "expected message array to be frozen"
    msg.each_with_index do |part, i|
      assert part.frozen?, "expected part #{i} to be frozen"
      assert_equal Encoding::BINARY, part.encoding, "expected part #{i} BINARY-tagged"
    end
  end


  [
    ["inproc", -> { "ruby://frozen-#{SecureRandom.hex(4)}" }],
    ["ipc",    -> { "ipc://#{Dir.tmpdir}/omq-frozen-#{SecureRandom.hex(4)}.sock" }],
    ["tcp",    -> { "tcp://127.0.0.1:0" }],
  ].each do |transport, ep_builder|
    it "freezes single-frame messages over #{transport}" do
      Async do
        pull = OMQ::PULL.new
        uri  = pull.bind(ep_builder.call).to_s
        push = OMQ::PUSH.new
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect(uri)
        wait_connected(push, pull) unless transport == "inproc"

        push.send("hello")
        msg = pull.receive

        assert_equal ["hello"], msg
        assert_frozen_message(msg)
      ensure
        push&.close
        pull&.close
      end
    end


    it "freezes multi-frame messages over #{transport}" do
      Async do
        pull = OMQ::PULL.new
        uri  = pull.bind(ep_builder.call).to_s
        push = OMQ::PUSH.new
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect(uri)
        wait_connected(push, pull) unless transport == "inproc"

        push.send(["a", "b", "c"])
        msg = pull.receive

        assert_equal ["a", "b", "c"], msg
        assert_frozen_message(msg)
      ensure
        push&.close
        pull&.close
      end
    end
  end


  it "upgrades frozen non-BINARY parts to BINARY on inproc" do
    Async do
      pull = OMQ::PULL.new
      uri  = pull.bind("ruby://frozen-nonbin-#{SecureRandom.hex(4)}").to_s
      push = OMQ::PUSH.new
      push.connect(uri)

      # Frozen UTF-8 literal — what every "# frozen_string_literal: true"
      # codebase produces. Writable#send can't re-tag it in place, so
      # the inproc path must copy it into a BINARY string.
      frozen_utf8 = "héllo".freeze
      assert frozen_utf8.frozen?
      refute_equal Encoding::BINARY, frozen_utf8.encoding

      push.send(frozen_utf8)
      msg = pull.receive

      assert_frozen_message(msg)
      assert_equal frozen_utf8.bytes, msg.first.bytes
      refute_equal Encoding::BINARY, frozen_utf8.encoding,
        "caller's string encoding must not be mutated"
    ensure
      push&.close
      pull&.close
    end
  end


  it "coerces String-like parts via #to_str" do
    Async do
      pull = OMQ::PULL.new
      uri  = pull.bind("ruby://to_str-#{SecureRandom.hex(4)}").to_s
      push = OMQ::PUSH.new
      push.connect(uri)

      stringy = Class.new do
        def initialize(s)
          @s = s
        end


        def to_str
          @s
        end

      end

      push.send(stringy.new("wrapped"))
      msg = pull.receive

      assert_frozen_message(msg)
      assert_equal ["wrapped"], msg
    ensure
      push&.close
      pull&.close
    end
  end


  it "raises on non-String-like parts (including nil)" do
    Async do
      pull = OMQ::PULL.new
      uri  = pull.bind("ruby://bad-part-#{SecureRandom.hex(4)}").to_s
      push = OMQ::PUSH.new
      push.connect(uri)

      assert_raises(NoMethodError) { push.send(42) }
      assert_raises(NoMethodError) { push.send([:not_stringy]) }
      assert_raises(NoMethodError) { push.send(nil) }
      assert_raises(NoMethodError) { push.send(["ok", nil]) }
    ensure
      push&.close
      pull&.close
    end
  end


  it "re-tags unfrozen non-BINARY parts in place via #send" do
    Async do
      pull = OMQ::PULL.new
      uri  = pull.bind("ruby://unfrozen-nonbin-#{SecureRandom.hex(4)}").to_s
      push = OMQ::PUSH.new
      push.connect(uri)

      utf8 = String.new("héllo", encoding: Encoding::UTF_8)
      refute utf8.frozen?
      refute_equal Encoding::BINARY, utf8.encoding

      push.send(utf8)
      msg = pull.receive

      assert_frozen_message(msg)
      assert utf8.frozen?, "expected caller's string to be frozen after send"
      assert_equal Encoding::BINARY, utf8.encoding, "expected caller's string to be BINARY-tagged after send"
    ensure
      push&.close
      pull&.close
    end
  end

end
