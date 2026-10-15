# frozen_string_literal: true

require_relative "../../test_helper"
require "omq/radio_dish"

class OMQTransportUDPTest < Minitest::Spec
  describe "UDP transport" do
    it "delivers a message to a joined group" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15550")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15550")
        dish.join("weather")
        Async::Task.current.yield

        radio.publish("weather", "sunny")
        assert_equal ["weather", "sunny"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end

    it "filters messages to un-joined groups" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15551")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15551")
        dish.join("weather")
        Async::Task.current.yield

        radio.publish("sports", "goal!")
        radio.publish("weather", "rainy")

        assert_equal ["weather", "rainy"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end

    it "supports group: kwarg on RADIO#send" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15552")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15552")
        dish.join("news")
        Async::Task.current.yield

        radio.send("headline", group: "news")
        assert_equal ["news", "headline"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end

    it "supports << with [group, body] on RADIO" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15553")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15553")
        dish.join("alerts")
        Async::Task.current.yield

        radio << ["alerts", "fire"]
        assert_equal ["alerts", "fire"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end

    it "stops delivering after leave" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15554")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15554")
        dish.join("weather")
        Async::Task.current.yield

        radio.publish("weather", "first")
        assert_equal ["weather", "first"], dish.receive

        dish.leave("weather")
        Async::Task.current.yield

        radio.publish("weather", "second")

        dish.read_timeout = 0.1
        assert_raises(IO::TimeoutError) { dish.receive }
      ensure
        radio&.close
        dish&.close
      end
    end

    it "receives from multiple groups" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15555")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15555")
        dish.join("a")
        dish.join("b")
        Async::Task.current.yield

        radio.publish("a", "msg-a")
        radio.publish("b", "msg-b")

        received = [dish.receive, dish.receive].sort_by(&:first)
        assert_equal [["a", "msg-a"], ["b", "msg-b"]], received
      ensure
        radio&.close
        dish&.close
      end
    end

    it "supports group: kwarg in DISH constructor" do
      Sync do
        dish  = OMQ::DISH.bind("udp://127.0.0.1:15556", group: "data")
        radio = OMQ::RADIO.connect("udp://127.0.0.1:15556")
        Async::Task.current.yield

        radio.publish("data", "payload")
        assert_equal ["data", "payload"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end
  end
end
