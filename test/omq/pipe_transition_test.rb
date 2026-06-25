# frozen_string_literal: true

require_relative "../test_helper"

describe "Pipe" do
  it "#encrypted? returns false" do
    pipe = OMQ::Transport::Inproc::Pipe.new(peer_identity: "", peer_type: "PUSH")
    refute pipe.encrypted?
  end
end

describe "direct pipe transitions" do
  before { OMQ::Transport::Inproc.reset! }

  it "falls back to send pump when a second inproc peer connects" do
    Async do
      push = OMQ::PUSH.new
      push.bind("ruby://dp-multi-#{object_id}")

      pull1 = OMQ::PULL.connect("ruby://dp-multi-#{object_id}")
      push << "solo"
      assert_equal ["solo"], pull1.receive

      pull2 = OMQ::PULL.connect("ruby://dp-multi-#{object_id}")
      pull1.read_timeout = 0.02
      pull2.read_timeout = 0.02
      4.times { |i| push << "rr-#{i}" }
      received = 0
      loop do
        pull1.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      loop do
        pull2.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      assert_equal 4, received
    ensure
      push&.close
      pull1&.close
      pull2&.close
    end
  end


  it "re-enables direct pipe when second peer disconnects" do
    Async do
      push = OMQ::PUSH.new
      push.bind("ruby://dp-reenable-#{object_id}")

      pull1 = OMQ::PULL.connect("ruby://dp-reenable-#{object_id}")
      pull2 = OMQ::PULL.connect("ruby://dp-reenable-#{object_id}")

      pull1.read_timeout = 0.02
      pull2.read_timeout = 0.02
      2.times { |i| push << "multi-#{i}" }
      received = 0
      loop do
        pull1.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      loop do
        pull2.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      assert_equal 2, received

      pull2.close
      pull2 = nil

      push << "back-to-direct"
      assert_equal ["back-to-direct"], pull1.receive
    ensure
      push&.close
      pull1&.close
      pull2&.close
    end
  end


  it "REQ/REP works over inproc with direct pipe bypass" do
    Async do
      rep = OMQ::REP.bind("ruby://dp-reqrep-#{object_id}")
      req = OMQ::REQ.connect("ruby://dp-reqrep-#{object_id}")

      req << "hello"
      assert_equal ["hello"], rep.receive
      rep << "world"
      assert_equal ["world"], req.receive
    ensure
      req&.close
      rep&.close
    end
  end


  it "DEALER/ROUTER works over inproc with direct pipe bypass" do
    Async do
      router = OMQ::ROUTER.bind("ruby://dp-dealer-#{object_id}")
      dealer = OMQ::DEALER.new
      dealer.identity = "test-dealer"
      dealer.connect("ruby://dp-dealer-#{object_id}")

      dealer << "hello"
      msg = router.receive
      assert_equal "test-dealer", msg[0]
      assert_equal "hello", msg[1]
    ensure
      dealer&.close
      router&.close
    end
  end
end
