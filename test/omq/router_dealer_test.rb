# frozen_string_literal: true

require_relative "../test_helper"

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
end
