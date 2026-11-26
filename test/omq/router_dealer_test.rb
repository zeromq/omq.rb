# frozen_string_literal: true

require_relative "../test_helper"

class OMQRouterDealerTest < Minitest::Spec
  describe "DEALER/ROUTER over inproc" do
    before { OMQ::Transport::Inproc.reset! }

    it "routes messages by identity" do
      Async do
        router = OMQ::ROUTER.bind("ruby://dealerrouter-1")
        dealer = OMQ::DEALER.new
        dealer.options.identity = "dealer-1"
        dealer.connect("ruby://dealerrouter-1")

        dealer.send("hello from dealer")
        msg = router.receive
        # ROUTER prepends identity frame
        assert_equal "dealer-1", msg[0]
        assert_equal "hello from dealer", msg[1]

        # Route reply back using identity
        router.send_to(msg[0], "hello back")
        reply = dealer.receive
        # DEALER sees the empty delimiter + message
        assert_equal ["", "hello back"], reply
      ensure
        dealer&.close
        router&.close
      end
    end

    it "silently drops messages to unknown identity by default" do
      Async do
        router = OMQ::ROUTER.bind("ruby://rm-1")
        dealer = OMQ::DEALER.new
        dealer.identity = "known"
        dealer.connect("ruby://rm-1")

        # Send to unknown identity — should not raise
        router.send(["unknown-peer", "", "hello"])

        # Known identity still works
        router.send_to("known", "hi")
        msg = dealer.receive
        assert_includes msg, "hi"
      ensure
        dealer&.close
        router&.close
      end
    end

    it "raises SocketError synchronously with router_mandatory" do
      Async do
        router = OMQ::ROUTER.bind("ruby://rm-2")
        router.router_mandatory = true

        assert_raises(SocketError) do
          router.send(["nonexistent", "", "hello"])
        end

        # Router still works after the error
        dealer = OMQ::DEALER.new
        dealer.identity = "real"
        dealer.connect("ruby://rm-2")

        router.send_to("real", "works")
        msg = dealer.receive
        assert_includes msg, "works"
      ensure
        dealer&.close
        router&.close
      end
    end


    it "keeps routing to the peer that took over an identity after the old one leaves" do
      Async do |task|
        router = OMQ::ROUTER.bind("inproc://rd-handover")
        router.router_mandatory = true

        old_peer = OMQ::DEALER.new
        old_peer.identity = "worker"
        old_peer.connect("inproc://rd-handover")

        new_peer = OMQ::DEALER.new
        new_peer.identity = "worker"
        new_peer.connect("inproc://rd-handover")
        assert_equal 2, router.connection_count

        old_peer.close
        task.yield until router.connection_count == 1

        router.send(["worker", "still routable"])
        assert_equal ["still routable"], new_peer.receive
      ensure
        new_peer&.close
        router&.close
      end
    end

    it "unblocks a send to a peer that leaves while its queue is full" do
      Async do |task|
        router = OMQ::ROUTER.bind("inproc://rd-departed-block")
        router.send_hwm = 1

        dealer = OMQ::DEALER.new(linger: 0)
        dealer.identity = "slow"
        dealer.recv_hwm = 1
        dealer.connect("inproc://rd-departed-block")

        sent  = 0
        flood = task.async do
          100.times do
            router.send(["slow", "x"])
            sent += 1
          end
        end

        # Run the flood until it stops making progress: it is now
        # blocked at HWM on the stalled peer's queue.
        last = -1
        until sent == last
          last = sent
          10.times { task.yield }
        end

        assert_operator sent, :<, 100
        dealer.close

        flood.wait
        assert_equal 100, sent
      ensure
        flood&.stop
        router&.close
      end
    end
  end
end
