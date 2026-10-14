# frozen_string_literal: true

require_relative "../test_helper"

class OMQReqRepTest < Minitest::Spec
  describe "REQ/REP over inproc" do
    before { OMQ::Transport::Inproc.reset! }

    it "completes a request-reply cycle" do
      Async do
        rep = OMQ::REP.bind("ruby://reqrep-1")
        req = OMQ::REQ.connect("ruby://reqrep-1")

        req.send("request")
        request = rep.receive
        assert_equal ["request"], request

        rep.send("reply")
        reply = req.receive
        assert_equal ["reply"], reply
      ensure
        req&.close
        rep&.close
      end.wait
    end

    it "handles multi-frame request/reply" do
      Async do
        rep = OMQ::REP.bind("ruby://reqrep-2")
        req = OMQ::REQ.connect("ruby://reqrep-2")

        req.send(["part1", "part2"])
        request = rep.receive
        assert_equal ["part1", "part2"], request

        rep.send(["reply1", "reply2"])
        reply = req.receive
        assert_equal ["reply1", "reply2"], reply
      ensure
        req&.close
        rep&.close
      end.wait
    end


    it "preserves multiple routing identity frames through REP" do
      Async do
        dealer = OMQ::DEALER.bind("ruby://reqrep-dealer-#{object_id}")
        rep    = OMQ::REP.new
        rep.connect("ruby://reqrep-dealer-#{object_id}")
        wait_connected(dealer, rep)

        dealer.send(["client-a", "client-b", "", "request"])
        assert_equal ["request"], rep.receive

        rep.send("reply")
        assert_equal ["client-a", "client-b", "", "reply"], dealer.receive
      ensure
        rep&.close
        dealer&.close
      end.wait
    end

  end
end
