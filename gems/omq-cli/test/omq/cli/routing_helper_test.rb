# frozen_string_literal: true

require_relative "../../test_helper"

describe "Routing helpers" do
  before do
    @runner = OMQ::CLI::ServerRunner.new(
      make_config(type_name: "server"),
      OMQ::SERVER
    )
  end

  describe "#display_routing_id" do
    it "passes through printable ASCII" do
      assert_equal "worker-1", @runner.send(:display_routing_id, "worker-1")
    end

    it "hex-encodes binary IDs" do
      assert_equal "0xdeadbeef", @runner.send(:display_routing_id, "\xDE\xAD\xBE\xEF".b)
    end

    it "hex-encodes IDs with leading zero" do
      assert_equal "0x00abcdef42", @runner.send(:display_routing_id, "\x00\xAB\xCD\xEF\x42".b)
    end

    it "hex-encodes IDs containing a mix of printable and non-printable" do
      id = "ab\x00cd".b
      displayed = @runner.send(:display_routing_id, id)
      assert displayed.start_with?("0x"), "expected hex encoding for mixed ID"
    end

    it "handles empty string" do
      assert_equal "", @runner.send(:display_routing_id, "")
    end
  end

  describe "#resolve_target" do
    it "decodes 0x-prefixed hex" do
      assert_equal "\xDE\xAD\xBE\xEF".b, @runner.send(:resolve_target, "0xdeadbeef")
    end

    it "decodes uppercase hex" do
      assert_equal "\xDE\xAD".b, @runner.send(:resolve_target, "0xDEAD")
    end

    it "strips spaces in hex" do
      assert_equal "\xDE\xAD\xBE\xEF".b, @runner.send(:resolve_target, "0xde ad be ef")
    end

    it "passes through plain text" do
      assert_equal "worker-1", @runner.send(:resolve_target, "worker-1")
    end

    it "passes through text that looks like hex but has no 0x prefix" do
      assert_equal "deadbeef", @runner.send(:resolve_target, "deadbeef")
    end
  end

  describe "round-trip" do
    it "round-trips 4-byte binary routing ID" do
      original  = "\xBF\x5D\x07\x01".b
      displayed = @runner.send(:display_routing_id, original)
      resolved  = @runner.send(:resolve_target, displayed)
      assert_equal original, resolved
    end

    it "round-trips 5-byte binary routing ID" do
      original  = "\x00\xAB\xCD\xEF\x42".b
      displayed = @runner.send(:display_routing_id, original)
      resolved  = @runner.send(:resolve_target, displayed)
      assert_equal original, resolved
    end

    it "round-trips ASCII identity" do
      original  = "my-worker"
      displayed = @runner.send(:display_routing_id, original)
      resolved  = @runner.send(:resolve_target, displayed)
      assert_equal original, resolved
    end
  end
end
