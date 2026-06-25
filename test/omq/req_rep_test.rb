# frozen_string_literal: true

require_relative "../test_helper"

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
    end
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
    end
  end

end
