# frozen_string_literal: true

require_relative "../test_helper"
require "omq/radio_dish"

describe "RADIO/DISH over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  it "delivers messages to joined groups" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-1")
      dish  = OMQ::DISH.connect("ruby://rd-1")
      dish.join("weather")

      Async::Task.current.yield

      radio.publish("weather", "72F")
      msg = dish.receive
      assert_equal ["weather", "72F"], msg
    ensure
      dish&.close
      radio&.close
    end
  end

  it "filters by exact group match" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-2")
      dish  = OMQ::DISH.connect("ruby://rd-2")
      dish.join("weather")

      Async::Task.current.yield

      radio.publish("sports", "goal!")
      radio.publish("weather", "sunny")

      msg = dish.receive
      assert_equal ["weather", "sunny"], msg
    ensure
      dish&.close
      radio&.close
    end
  end

  it "supports send with group: kwarg" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-3")
      dish  = OMQ::DISH.connect("ruby://rd-3")
      dish.join("news")

      Async::Task.current.yield

      radio.send("headline", group: "news")
      msg = dish.receive
      assert_equal ["news", "headline"], msg
    ensure
      dish&.close
      radio&.close
    end
  end

  it "supports << with [group, body] array" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-4")
      dish  = OMQ::DISH.connect("ruby://rd-4")
      dish.join("alerts")

      Async::Task.current.yield

      radio << ["alerts", "fire"]
      msg = dish.receive
      assert_equal ["alerts", "fire"], msg
    ensure
      dish&.close
      radio&.close
    end
  end

  it "stops delivering after leave" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-5")
      dish  = OMQ::DISH.connect("ruby://rd-5")
      dish.join("weather")

      Async::Task.current.yield

      radio.publish("weather", "first")
      assert_equal ["weather", "first"], dish.receive

      dish.leave("weather")
      Async::Task.current.yield

      radio.publish("weather", "second")

      # Should not receive — set a short timeout
      dish.read_timeout = 0.05
      assert_raises(IO::TimeoutError) { dish.receive }
    ensure
      dish&.close
      radio&.close
    end
  end

  it "supports group: kwarg in DISH constructor" do
    Sync do
      radio = OMQ::RADIO.bind("ruby://rd-6")
      dish  = OMQ::DISH.connect("ruby://rd-6", group: "data")

      Async::Task.current.yield

      radio.publish("data", "payload")
      msg = dish.receive
      assert_equal ["data", "payload"], msg
    ensure
      dish&.close
      radio&.close
    end
  end
end
