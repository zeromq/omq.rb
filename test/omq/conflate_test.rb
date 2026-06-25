# frozen_string_literal: true

require_relative "../test_helper"

describe "PUB conflate" do
  before { OMQ::Transport::Inproc.reset! }

  it "delivers only the latest message when conflate is enabled" do
    Async do
      pub = OMQ::PUB.new(nil, conflate: true)
      pub.bind("ruby://conflate-pub")

      sub = OMQ::SUB.connect("ruby://conflate-pub", subscribe: "")

      # Burst: many updates
      100.times { |i| pub.send("msg-#{i}") }

      sub.recv_timeout = 0.02
      received = []
      loop do
        received << sub.receive.first
      rescue IO::TimeoutError
        break
      end

      assert_operator received.size, :<, 100, "conflate should reduce message count"
      assert_equal "msg-99", received.last
    ensure
      pub&.close
      sub&.close
    end
  end

  it "delivers all messages when conflate is disabled" do
    Async do
      pub = OMQ::PUB.bind("ruby://no-conflate-pub")
      sub = OMQ::SUB.connect("ruby://no-conflate-pub", subscribe: "")

      10.times { |i| pub.send("msg-#{i}") }

      sub.recv_timeout = 0.02
      received = []
      loop do
        received << sub.receive.first
      rescue IO::TimeoutError
        break
      end

      assert_equal 10, received.size
    ensure
      pub&.close
      sub&.close
    end
  end
end

describe "RADIO conflate" do
  before { OMQ::Transport::Inproc.reset! }

  it "delivers only the latest message when conflate is enabled" do
    Async do
      radio = OMQ::RADIO.new(nil, conflate: true)
      radio.bind("ruby://conflate-radio")

      dish = OMQ::DISH.new(nil, group: "sensor")
      dish.connect("ruby://conflate-radio")

      100.times { |i| radio.publish("sensor", "value-#{i}") }

      dish.recv_timeout = 0.02
      received = []
      loop do
        received << dish.receive.last
      rescue IO::TimeoutError
        break
      end

      assert_operator received.size, :<, 100, "conflate should reduce message count"
      assert_equal "value-99", received.last
    ensure
      radio&.close
      dish&.close
    end
  end
end
