# frozen_string_literal: true

require_relative "../test_helper"
require "omq/radio_dish"

# Disconnect-then-reconnect must work for RADIO/DISH.
# The connecting side detects peer loss and auto-reconnects.
#
class OMQReconnectRadioDishTest < Minitest::Spec
  describe "Reconnect after server restart — RADIO/DISH" do
    it "RADIO/DISH" do
      Sync do
        radio = OMQ::RADIO.new(nil, linger: 0)
        port = radio.bind("tcp://127.0.0.1:0").port

        dish = OMQ::DISH.new(nil, linger: 0)
        dish.reconnect_interval = RECONNECT_INTERVAL
        dish.connect("tcp://127.0.0.1:#{port}")
        dish.join("g")
        wait_connected(dish, radio)
        sleep 0.01 # JOIN command propagation

        radio.publish("g", "first")
        assert_equal ["g", "first"], dish.receive

        radio.close
        sleep 0.02

        radio2 = OMQ::RADIO.new(nil, linger: 0)
        radio2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(dish, radio2)
        sleep 0.01 # JOIN command propagation

        radio2.publish("g", "second")
        assert_equal ["g", "second"], dish.receive
      ensure
        dish&.close
        radio2&.close
      end
    end
  end
end
