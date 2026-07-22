# frozen_string_literal: true

require_relative "../test_helper"

describe Protocol::ZMTP::Codec::Subscription do
  Subscription = Protocol::ZMTP::Codec::Subscription
  Frame        = Protocol::ZMTP::Codec::Frame
  Command      = Protocol::ZMTP::Codec::Command

  describe ".body" do
    it "builds a subscribe body with 0x01 prefix" do
      body = Subscription.body("topic")
      assert_equal "\x01topic".b, body
      assert_equal Encoding::BINARY, body.encoding
    end

    it "builds a cancel body with 0x00 prefix" do
      body = Subscription.body("topic", cancel: true)
      assert_equal "\x00topic".b, body
    end

    it "handles an empty prefix (subscribe-all)" do
      assert_equal "\x01".b, Subscription.body("")
    end
  end


  describe ".parse" do
    it "recognizes message-form subscribe" do
      frame = Frame.new(Subscription.body("topic"))
      assert_equal [:subscribe, "topic".b], Subscription.parse(frame)
    end

    it "recognizes message-form cancel" do
      frame = Frame.new(Subscription.body("topic", cancel: true))
      assert_equal [:cancel, "topic".b], Subscription.parse(frame)
    end

    it "recognizes command-form SUBSCRIBE" do
      cmd   = Command.subscribe("topic")
      frame = cmd.to_frame
      assert_equal [:subscribe, "topic".b], Subscription.parse(frame)
    end

    it "recognizes command-form CANCEL" do
      cmd   = Command.cancel("topic")
      frame = cmd.to_frame
      assert_equal [:cancel, "topic".b], Subscription.parse(frame)
    end

    it "returns nil for an empty data frame" do
      assert_nil Subscription.parse(Frame.new(""))
    end

    it "returns nil for a data frame with a non-sub flag byte" do
      assert_nil Subscription.parse(Frame.new("\x02data"))
    end

    it "returns nil for a non-subscription command" do
      cmd = Command.ready(socket_type: "REQ")
      assert_nil Subscription.parse(cmd.to_frame)
    end
  end
end
