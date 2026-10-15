# frozen_string_literal: true

require_relative "../../test_helper"

class OMQEngineSocketLifecycleTest < Minitest::Spec
  describe OMQ::Engine::SocketLifecycle do
    let(:lc) { OMQ::Engine::SocketLifecycle.new }

    it "starts in :new with unresolved promises and default flags" do
      assert_equal :new, lc.state
      refute lc.peer_connected.resolved?
      refute lc.all_peers_gone.resolved?
      assert lc.reconnect_enabled
      assert lc.alive?
      refute lc.open?
      refute lc.closed?
    end

    it "rejects transitions that skip required states" do
      assert_raises(OMQ::Engine::SocketLifecycle::InvalidTransition) do
        lc.start_closing!   # :new → :closing not allowed
      end
    end

    describe "#capture_parent_task" do
      it "captures the current Async task and transitions to :open" do
        Async do
          assert lc.capture_parent_task(linger: 0)
          assert lc.open?
          refute lc.on_io_thread
          refute_nil lc.parent_task
        end
      end

      it "returns false on a second call and does not re-transition" do
        Async do
          lc.capture_parent_task(linger: 0)
          refute lc.capture_parent_task(linger: 0)
          assert lc.open?
        end
      end
    end

    describe "close transitions" do
      it "goes :open → :closing → :closed" do
        Async do
          lc.capture_parent_task(linger: 0)
          lc.start_closing!
          assert lc.closing?
          lc.finish_closing!
          assert lc.closed?
          refute lc.alive?
        end
      end

      it "cannot re-open after :closed" do
        Async do
          lc.capture_parent_task(linger: 0)
          lc.start_closing!
          lc.finish_closing!
          assert_raises(OMQ::Engine::SocketLifecycle::InvalidTransition) do
            lc.start_closing!
          end
        end
      end
    end

    describe "#maybe_resolve_all_peers_gone" do
      it "no-ops if peer_connected never resolved" do
        lc.maybe_resolve_all_peers_gone({})
        refute lc.all_peers_gone.resolved?
      end

      it "no-ops if connections still present" do
        lc.peer_connected.resolve(:fake)
        lc.maybe_resolve_all_peers_gone({ :fake => :lifecycle })
        refute lc.all_peers_gone.resolved?
      end

      it "resolves once we had peers and the map is empty" do
        lc.peer_connected.resolve(:fake)
        lc.maybe_resolve_all_peers_gone({})
        assert lc.all_peers_gone.resolved?
      end
    end
  end
end
