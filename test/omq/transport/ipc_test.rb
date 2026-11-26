# frozen_string_literal: true

require_relative "../../test_helper"
require "fileutils"
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

    it "returns abstract namespace URIs that keep the @" do
      skip "abstract namespace only on Linux" unless RUBY_PLATFORM =~ /linux/

      Async do
        endpoint = "ipc://@omq-test-abstract-uri-#{$$}"
        server   = OMQ::PAIR.new
        client   = OMQ::PAIR.new
        bound    = server.bind(endpoint)
        assert_equal endpoint, bound.to_s
        assert_equal "@omq-test-abstract-uri-#{$$}", bound.path

        assert_equal endpoint, client.connect(bound.to_s).to_s
        client.send("via the bound URI")
        assert_equal ["via the bound URI"], server.receive
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


    describe "socket file ownership" do
      def sock_path(name)
        File.join(Dir.tmpdir, "omq-test-#{name}-#{$$}.sock")
      end


      it "refuses to replace a regular file" do
        path = sock_path("regular")
        File.write(path, "keep me")

        assert_raises(Errno::EADDRINUSE) { OMQ::PULL.bind("ipc://#{path}") }
        assert_equal "keep me", File.read(path)
      ensure
        File.delete(path) rescue nil
      end


      it "refuses to steal a socket that is still listening" do
        Async do
          path  = sock_path("live")
          first = OMQ::PULL.bind("ipc://#{path}")

          assert_raises(Errno::EADDRINUSE) { OMQ::PULL.bind("ipc://#{path}") }

          push = OMQ::PUSH.connect("ipc://#{path}")
          push << "still first"
          assert_equal ["still first"], first.receive
        ensure
          push&.close
          first&.close
        end
      end


      it "replaces a stale socket file" do
        Async do
          path = sock_path("stale")
          UNIXServer.new(path).close # leaves the file behind
          assert File.socket?(path)

          pull = OMQ::PULL.bind("ipc://#{path}")
          push = OMQ::PUSH.connect("ipc://#{path}")
          push << "rebound"
          assert_equal ["rebound"], pull.receive
        ensure
          push&.close
          pull&.close
        end
      end


      it "leaves a socket file that another listener bound since" do
        Async do
          path = sock_path("replaced")
          pull = OMQ::PULL.bind("ipc://#{path}")

          # Someone else unlinks and rebinds the path (libzmq does).
          File.delete(path)
          other = UNIXServer.new(path)

          pull.close
          assert File.socket?(path)
        ensure
          other&.close
          File.delete(path) rescue nil
        end
      end


      it "removes a relative socket file after a chdir" do
        Async do
          dir  = Dir.mktmpdir("omq-ipc-rel")
          pull = Dir.chdir(dir) { OMQ::PULL.bind("ipc://rel.sock") }

          pull.close
          refute File.exist?(File.join(dir, "rel.sock"))
        ensure
          FileUtils.rm_rf(dir) if dir
        end
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
