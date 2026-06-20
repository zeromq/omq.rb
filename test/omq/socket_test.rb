# frozen_string_literal: true

require_relative "../test_helper"

describe OMQ::Socket do
  before { OMQ::Transport::Inproc.reset! }

  describe "#inspect" do
    it "includes class name and bound endpoints" do
      Async do
        rep = OMQ::REP.bind("ruby://inspect-test")
        s = rep.inspect
        assert_match(/OMQ::REP/, s)
        assert_match(/ruby:\/\/inspect-test/, s)
      ensure
        rep&.close
      end
    end

    it "shows empty bound list before bind/connect" do
      Async do
        rep = OMQ::REP.new
        assert_match(/bound=\[\]/, rep.inspect)
      ensure
        rep&.close
      end
    end
  end

  describe "ØMQ alias" do
    it "is the same as OMQ" do
      assert_equal OMQ, ØMQ
      assert_equal OMQ::REQ, ØMQ::REQ
      assert_equal OMQ::PUB, ØMQ::PUB
    end
  end

  describe "empty and binary messages" do
    it "handles empty string message" do
      Async do
        pull = OMQ::PULL.bind("ruby://empty-msg")
        push = OMQ::PUSH.connect("ruby://empty-msg")

        push.send("")
        msg = pull.receive
        assert_equal [""], msg
      ensure
        push&.close
        pull&.close
      end
    end

    it "handles binary data with all 256 byte values" do
      Async do
        pull = OMQ::PULL.bind("ruby://binary-msg")
        push = OMQ::PUSH.connect("ruby://binary-msg")

        binary = (0..255).map(&:chr).join.b
        push.send(binary)
        msg = pull.receive
        assert_equal [binary], msg
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
