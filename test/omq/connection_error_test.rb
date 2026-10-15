# frozen_string_literal: true

require_relative "../test_helper"
require "socket"

class OMQConnectionErrorTest < Minitest::Spec
  describe "Connection error handling" do
    it "server survives client disconnect during TCP handshake" do
      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        port = rep.bind("tcp://127.0.0.1:0").port

        # Raw TCP connect + immediate close — triggers EPIPE/ECONNRESET
        # during the server's greeting write
        raw = TCPSocket.new("127.0.0.1", port)
        raw.close

        # Give the server time to handle the broken connection
        sleep 0.02

        # A real client should still be able to connect and exchange messages
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("tcp://127.0.0.1:#{port}")

        req.send("after reset")
        msg = rep.receive
        assert_equal ["after reset"], msg
      ensure
        req&.close
        rep&.close
      end
    end

    it "server survives client disconnect during IPC handshake" do
      path = "/tmp/omq_test_epipe_#{$$}.sock"

      Async do
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.bind("ipc://#{path}")

        # Raw connect + immediate close
        raw = UNIXSocket.new(path)
        raw.close

        sleep 0.02

        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.connect("ipc://#{path}")

        req.send("after reset")
        msg = rep.receive
        assert_equal ["after reset"], msg
      ensure
        req&.close
        rep&.close
        File.delete(path) rescue nil
      end
    end

    it "reconnects after IPC socket file is removed" do
      path = "/tmp/omq_test_reconnect_#{$$}.sock"

      Async do
        # Start server
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.bind("ipc://#{path}")

        # Client connects
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = RECONNECT_INTERVAL
        req.connect("ipc://#{path}")

        # First exchange works
        req.send("hello")
        msg = rep.receive
        assert_equal ["hello"], msg
        rep.send("world")
        reply = req.receive
        assert_equal ["world"], reply

        # Kill the server (removes socket file)
        rep.close

        sleep 0.02

        # Restart server on same path
        rep2 = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep2.bind("ipc://#{path}")

        # Wait for reconnection
        wait_connected(req, rep2)

        req.send("reconnected")
        msg = rep2.receive
        assert_equal ["reconnected"], msg
      ensure
        req&.close
        rep2&.close
        File.delete(path) rescue nil
      end
    end

    it "schedules reconnect on connection reset during TCP connect" do
      Async do
        # Client connects to a port that resets connections
        req = OMQ::REQ.new.tap { |s| s.linger = 0 }
        req.reconnect_interval = 0.05

        # Start a raw server that accepts and immediately resets
        server = TCPServer.new("127.0.0.1", 0)
        port   = server.local_address.ip_port

        resetter = Async do
          client = server.accept
          linger = [1, 0].pack("ii")
          client.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, linger)
          client.close
        end

        req.connect("tcp://127.0.0.1:#{port}")

        # Wait for the reset + reconnect attempt
        sleep 0.02
        resetter.wait
        server.close

        # Now start a real server on the same port
        rep = OMQ::REP.new.tap { |s| s.linger = 0 }
        rep.bind("tcp://127.0.0.1:#{port}")

        sleep 0.08

        req.send("recovered")
        msg = rep.receive
        assert_equal ["recovered"], msg
      ensure
        req&.close
        rep&.close
        server&.close rescue nil
      end
    end
  end
end
