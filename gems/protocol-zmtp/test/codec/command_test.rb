# frozen_string_literal: true

require_relative "../test_helper"

class ProtocolZMTPCodecCommandTest < Minitest::Spec
  describe Protocol::ZMTP::Codec::Command do
    Command = Protocol::ZMTP::Codec::Command

    describe ".ready" do
      it "creates a READY command with socket type" do
        cmd = Command.ready(socket_type: "REQ")
        assert_equal "READY", cmd.name
      end

      it "encodes Socket-Type and Identity properties" do
        cmd = Command.ready(socket_type: "DEALER", identity: "my-id")
        props = cmd.properties
        assert_equal "DEALER", props["Socket-Type"]
        assert_equal "my-id", props["Identity"]
      end

      it "defaults identity to empty string" do
        cmd = Command.ready(socket_type: "PUB")
        assert_equal "", cmd.properties["Identity"]
      end
    end

    describe ".subscribe / .cancel" do
      it "creates SUBSCRIBE command" do
        cmd = Command.subscribe("topic.")
        assert_equal "SUBSCRIBE", cmd.name
        assert_equal "topic.".b, cmd.data
      end

      it "creates CANCEL command" do
        cmd = Command.cancel("topic.")
        assert_equal "CANCEL", cmd.name
        assert_equal "topic.".b, cmd.data
      end

      it "handles empty prefix" do
        cmd = Command.subscribe("")
        assert_equal "SUBSCRIBE", cmd.name
        assert_equal "".b, cmd.data
      end
    end

    describe ".ping / .pong" do
      it "rejects truncated TTLs and oversized contexts" do
        ["", "\x00", "\x00\x01" + "x" * 17].each do |data|
          assert_raises(Protocol::ZMTP::Error) { Command.new("PING", data).ping_ttl_and_context }
        end
        assert_raises(Protocol::ZMTP::Error) { Command.ping(context: "x" * 17) }
        assert_raises(Protocol::ZMTP::Error) { Command.pong(context: "x" * 17) }
      end

      it "checks TTL bounds and supports the largest valid PING" do
        assert_raises(Protocol::ZMTP::Error) { Command.ping(ttl: -1) }
        assert_raises(Protocol::ZMTP::Error) { Command.ping(ttl: 6553.6) }
        assert_equal [6553.5, "x" * 16], Command.ping(ttl: 6553.5, context: "x" * 16).ping_ttl_and_context
      end

      it "roundtrips a PING command with TTL" do
        cmd = Command.ping(ttl: 3.0, context: "ctx")
        decoded = Command.from_body(cmd.to_body)
        assert_equal "PING", decoded.name
        ttl, context = decoded.ping_ttl_and_context
        assert_equal 3.0, ttl
        assert_equal "ctx", context
      end
    end

    describe "handshake errors" do
      it "raises a fatal error with the peer's reason" do
        cmd = Command.from_body(Command.error("denied").to_body)
        err = assert_raises(Protocol::ZMTP::FatalHandshakeError) { cmd.raise_if_error! }
        assert_includes err.message, "denied"
      end

      it "treats malformed ERROR reasons as fatal" do
        ["", "\x06short", "\x00extra"].each do |data|
          assert_raises(Protocol::ZMTP::FatalHandshakeError) { Command.new("ERROR", data).raise_if_error! }
        end
      end
    end

    describe "#to_body / .from_body round-trip" do
      it "round-trips a READY command" do
        original = Command.ready(socket_type: "ROUTER", identity: "test-123")
        decoded  = Command.from_body(original.to_body)
        assert_equal "READY", decoded.name
        assert_equal "ROUTER", decoded.properties["Socket-Type"]
        assert_equal "test-123", decoded.properties["Identity"]
      end

      it "round-trips a SUBSCRIBE command" do
        original = Command.subscribe("weather.")
        decoded  = Command.from_body(original.to_body)
        assert_equal "SUBSCRIBE", decoded.name
        assert_equal "weather.".b, decoded.data
      end
    end

    describe "#to_frame" do
      it "produces a Frame with command flag set" do
        frame = Command.ready(socket_type: "REQ").to_frame
        assert frame.command?
        refute frame.more?
      end
    end

    describe ".from_body" do
      it "raises on empty body" do
        assert_raises(Protocol::ZMTP::Error) { Command.from_body("".b) }
      end

      it "raises on truncated name" do
        assert_raises(Protocol::ZMTP::Error) { Command.from_body("\x0Aabc".b) }
      end
    end

    describe "#properties" do
      it "looks up metadata case-insensitively without changing its spelling" do
        props = { "sOcKeT-tYpE" => "REQ", "IDENTITY" => "peer", "x-Custom" => "value" }
        decoded = Command.decode_properties(Command.encode_properties(props))
        assert_equal props, decoded
        assert_equal "REQ", Command.property(decoded, "Socket-Type")
        assert_equal "peer", Command.property(decoded, "Identity")
        assert_equal "value", Command.property(decoded, "X-CUSTOM")
        assert_nil Command.property(decoded, "absent")
      end

      it "parses multiple properties" do
        cmd = Command.ready(socket_type: "SUB", identity: "id-42")
        props = cmd.properties
        assert_equal 2, props.size
        assert_equal "SUB", props["Socket-Type"]
        assert_equal "id-42", props["Identity"]
      end

      it "encodes and decodes properties" do
        props   = { "Socket-Type" => "PAIR", "Identity" => "" }
        encoded = Command.encode_properties(props)
        decoded = Command.decode_properties(encoded)
        assert_equal props, decoded
      end

      it "raises on truncated property name" do
        cmd = Command.new("READY", "\x05AB".b)
        assert_raises(Protocol::ZMTP::Error) { cmd.properties }
      end

      it "raises on truncated property value length" do
        cmd = Command.new("READY", "\x01X\x00".b)
        assert_raises(Protocol::ZMTP::Error) { cmd.properties }
      end
    end
  end
end
