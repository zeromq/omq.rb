# frozen_string_literal: true

require_relative "../test_helper"
require "omq/scatter_gather"

describe "SCATTER/GATHER over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  it "sends and receives messages" do
    Sync do
      gather  = OMQ::GATHER.bind("ruby://sg-1")
      scatter = OMQ::SCATTER.connect("ruby://sg-1")

      scatter.send("hello")
      msg = gather.receive
      assert_equal ["hello"], msg
    ensure
      scatter&.close
      gather&.close
    end
  end

  it "distributes across multiple GATHER peers" do
    Sync do
      g1 = OMQ::GATHER.bind("ruby://sg-rr-1")
      g2 = OMQ::GATHER.bind("ruby://sg-rr-2")

      scatter = OMQ::SCATTER.new
      scatter.connect("ruby://sg-rr-1")
      scatter.connect("ruby://sg-rr-2")

      n = 20
      n.times { |i| scatter.send("msg#{i}") }

      received = []
      barrier  = Async::Barrier.new
      [g1, g2].each do |g|
        g.read_timeout = 0.05
        barrier.async do
          loop do
            received << g.receive.first
          rescue IO::TimeoutError
            break
          end
        end
      end
      barrier.wait

      assert_equal n, received.size
      assert_equal n, received.uniq.size
    ensure
      scatter&.close
      g1&.close
      g2&.close
    end
  end

  it "rejects multipart messages" do
    Sync do
      gather  = OMQ::GATHER.bind("ruby://sg-mp")
      scatter = OMQ::SCATTER.connect("ruby://sg-mp")

      assert_raises(ArgumentError) { scatter.send(["part1", "part2"]) }
    ensure
      scatter&.close
      gather&.close
    end
  end
end
