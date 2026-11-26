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

    it "applies sndbuf and rcvbuf to dialed and accepted connections" do
      Async do
        pull = OMQ::PULL.new
        pull.sndbuf = 65_536
        pull.rcvbuf = 65_536
        pull.bind("ipc://@omq-test-bufsizes-#{$$}")

        push = OMQ::PUSH.new
        push.sndbuf = 65_536
        push.rcvbuf = 65_536
        push.connect("ipc://@omq-test-bufsizes-#{$$}")

        push.send("buffered")
        assert_equal ["buffered"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "keeps accepting after a transient accept error" do
      Async do
        pull = OMQ::PULL.new
        pull.reconnect_interval = RECONNECT_INTERVAL
        events = []
        pull.monitor { |event| events << event.type }
        pull.bind("ipc://@omq-test-accept-retry-#{$$}")
        server = pull.instance_variable_get(:@engine).listeners.values.first.server

        # The accept after the first client fails once with EMFILE.
        original = server.method(:accept)
        failed   = false
        server.define_singleton_method(:accept) do |*args|
          unless failed
            failed = true
            raise Errno::EMFILE
          end

          original.call(*args)
        end

        first = OMQ::PUSH.new(linger: 0)
        first.connect("ipc://@omq-test-accept-retry-#{$$}")
        first << "first"
        assert_equal ["first"], pull.receive

        second = OMQ::PUSH.new(linger: 0)
        second.connect("ipc://@omq-test-accept-retry-#{$$}")
        second << "second"
        assert_equal ["second"], pull.receive
        assert_includes events, :accept_failed
      ensure
        second&.close
        first&.close
        pull&.close
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
