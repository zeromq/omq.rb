# frozen_string_literal: true

require_relative "../test_helper"
require "omq/channel"

describe "CHANNEL over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  it "bidirectional communication" do
    Sync do
      a = OMQ::CHANNEL.bind("ruby://ch-1")
      b = OMQ::CHANNEL.connect("ruby://ch-1")

      b.send("from b")
      assert_equal ["from b"], a.receive

      a.send("from a")
      assert_equal ["from a"], b.receive
    ensure
      a&.close
      b&.close
    end
  end

  it "rejects multipart messages" do
    Sync do
      a = OMQ::CHANNEL.bind("ruby://ch-mp")
      b = OMQ::CHANNEL.connect("ruby://ch-mp")

      assert_raises(ArgumentError) { a.send(["part1", "part2"]) }
    ensure
      a&.close
      b&.close
    end
  end
end
