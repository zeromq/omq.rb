# frozen_string_literal: true

require_relative "../../test_helper"
require "tmpdir"

class OMQTransportIPCTest < Minitest::Spec
  describe "IPC transport" do
    it "PAIR over file-based IPC" do
      Async do
        path = File.join(Dir.tmpdir, "omq-test-#{$$}.sock")
        server = OMQ::PAIR.bind("ipc://#{path}")
        client = OMQ::PAIR.connect("ipc://#{path}")

        client.send("hello ipc")
        msg = server.receive
        assert_equal ["hello ipc"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "PAIR over abstract namespace IPC" do
      skip "abstract namespace only on Linux" unless RUBY_PLATFORM =~ /linux/

      Async do
        server = OMQ::PAIR.bind("ipc://@omq-test-abstract-#{$$}")
        client = OMQ::PAIR.connect("ipc://@omq-test-abstract-#{$$}")

        client.send("hello abstract")
        msg = server.receive
        assert_equal ["hello abstract"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "REQ/REP over IPC" do
      Async do
        path = File.join(Dir.tmpdir, "omq-test-reqrep-#{$$}.sock")
        rep = OMQ::REP.bind("ipc://#{path}")
        req = OMQ::REQ.connect("ipc://#{path}")

        req.send("request")
        assert_equal ["request"], rep.receive

        rep.send("reply")
        assert_equal ["reply"], req.receive
      ensure
        req&.close
        rep&.close
      end
    end
  end
end
