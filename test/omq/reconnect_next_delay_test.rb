# frozen_string_literal: true

require_relative "../test_helper"

class OMQReconnectNextDelayTest < Minitest::Spec
  describe "Reconnect#next_delay" do
    def build_reconnect(reconnect_interval)
      options = OMQ::Options.new
      options.reconnect_interval = reconnect_interval
      OMQ::Engine::Reconnect.send(:new, nil, "tcp://127.0.0.1:5555", options)
    end


    describe "plain Numeric (fixed interval, no backoff)" do
      it "returns the same interval every time" do
        r = build_reconnect(0.1)
        _delay, max = r.send(:init_delay, nil)
        assert_equal 0.1, _delay
        assert_nil max

        10.times do
          _delay = r.send(:next_delay, _delay, max)
          assert_equal 0.1, _delay
        end
      end
    end


    describe "Range (exponential backoff)" do
      it "doubles delay up to the max" do
        r = build_reconnect(0.1..2.0)
        delay, max = r.send(:init_delay, nil)
        assert_equal 0.1, delay
        assert_equal 2.0, max

        delay = r.send(:next_delay, delay, max)
        assert_equal 0.2, delay

        delay = r.send(:next_delay, delay, max)
        assert_equal 0.4, delay

        delay = r.send(:next_delay, delay, max)
        assert_equal 0.8, delay

        delay = r.send(:next_delay, delay, max)
        assert_equal 1.6, delay

        # Capped at max
        delay = r.send(:next_delay, delay, max)
        assert_equal 2.0, delay

        delay = r.send(:next_delay, delay, max)
        assert_equal 2.0, delay
      end


      it "recovers from zero initial delay (immediate first attempt)" do
        r = build_reconnect(0.05..1.0)
        delay = r.send(:next_delay, 0, 1.0)
        assert_equal 0.05, delay
      end
    end
  end


  describe "Reconnect#quantized_wait" do
    def build_reconnect
      options = OMQ::Options.new
      OMQ::Engine::Reconnect.send(:new, nil, "tcp://127.0.0.1:5555", options)
    end

    it "waits until the next wall-clock grid tick" do
      r = build_reconnect
      # t=12.3, delay=1.0 → wait = 1.0 - 0.3 = 0.7 → next tick at t=13.0
      assert_in_delta 0.7, r.send(:quantized_wait, 1.0, 12.3), 1e-9
    end

    it "returns the full delay when exactly on a grid boundary" do
      r = build_reconnect
      # t=10.0, delay=1.0 → 10.0 % 1.0 == 0 → wait = 1.0 - 0 = 1.0 (not 0)
      # guarding against the zero-wait degenerate case.
      assert_equal 1.0, r.send(:quantized_wait, 1.0, 10.0)
    end

    it "aligns sub-second intervals to a fine grid" do
      r = build_reconnect
      # t=5.17, delay=0.1 → 5.17 % 0.1 ≈ 0.07 → wait ≈ 0.03
      assert_in_delta 0.03, r.send(:quantized_wait, 0.1, 5.17), 1e-9
    end

    it "aligns clients that are out of phase to the same boundary" do
      r = build_reconnect
      # Two clients call at different moments within the same 1s window.
      # They should both wake at the same next-second boundary (43.0).
      wait_a = r.send(:quantized_wait, 1.0, 42.1)
      wait_b = r.send(:quantized_wait, 1.0, 42.8)
      assert_in_delta 43.0, 42.1 + wait_a, 1e-9
      assert_in_delta 43.0, 42.8 + wait_b, 1e-9
    end
  end
end
