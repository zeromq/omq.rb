# frozen_string_literal: true

require_relative "../test_helper"

describe OMQ::PAIR do
  before do
    OMQ::Transport::Inproc.reset!
  end

  it "sends and receives a message over inproc" do
    Async do
      server = OMQ::PAIR.bind("ruby://pair-test-1")
      client = OMQ::PAIR.connect("ruby://pair-test-1")

      client.send("hello")
      msg = server.receive
      assert_equal ["hello"], msg

      server.send("world")
      msg = client.receive
      assert_equal ["world"], msg
    ensure
      client&.close
      server&.close
    end
  end

  it "handles multi-frame messages" do
    Async do
      server = OMQ::PAIR.bind("ruby://pair-test-2")
      client = OMQ::PAIR.connect("ruby://pair-test-2")

      client.send(["part1", "part2", "part3"])
      msg = server.receive
      assert_equal ["part1", "part2", "part3"], msg
    ensure
      client&.close
      server&.close
    end
  end

  it "supports << for chaining" do
    Async do
      server = OMQ::PAIR.bind("ruby://pair-test-3")
      client = OMQ::PAIR.connect("ruby://pair-test-3")

      result = client << "test"
      assert_same client, result

      msg = server.receive
      assert_equal ["test"], msg
    ensure
      client&.close
      server&.close
    end
  end

  it "returns parsed URI from #bind" do
    Async do
      server = OMQ::PAIR.new
      uri = server.bind("ruby://pair-test-5")
      assert_equal "ruby://pair-test-5", uri.to_s
    ensure
      server&.close
    end
  end
end
