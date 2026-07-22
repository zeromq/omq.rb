# frozen_string_literal: true

require_relative "../test_helper"
require "socket"
require "io/stream"

describe Protocol::ZMTP::Mechanism::Plain do
  Plain      = Protocol::ZMTP::Mechanism::Plain
  Connection = Protocol::ZMTP::Connection

  def make_pair(server_mech:, client_mech:,
                server_type: "REP", client_type: "REQ")
    s1, s2 = UNIXSocket.pair
    server_io = IO::Stream::Buffered.wrap(s1)
    client_io = IO::Stream::Buffered.wrap(s2)

    server = Connection.new(server_io, socket_type: server_type, as_server: true,
                            mechanism: server_mech)
    client = Connection.new(client_io, socket_type: client_type, as_server: false,
                            mechanism: client_mech)

    [server, client, server_io, client_io]
  end

  it "is not encrypted" do
    refute Plain.new.encrypted?
  end

  it "completes handshake with no authenticator" do
    Async do
      server_mech = Plain.new
      client_mech = Plain.new(username: "alice", password: "s3cr3t")
      server, client, sio, cio = make_pair(server_mech: server_mech, client_mech: client_mech)

      Barrier do |bar|
        bar.async { server.handshake! }
        bar.async { client.handshake! }
      end

      assert_equal "REP", client.peer_socket_type
      assert_equal "REQ", server.peer_socket_type
    ensure
      sio&.close
      cio&.close
    end
  end

  it "passes credentials to the authenticator" do
    Async do
      received = []
      server_mech = Plain.new(authenticator: lambda { |u, p|
        received << [u, p]
        true
      })
      client_mech = Plain.new(username: "bob", password: "hunter2")
      server, client, sio, cio = make_pair(server_mech: server_mech, client_mech: client_mech)

      Barrier do |bar|
        bar.async { server.handshake! }
        bar.async { client.handshake! }
      end

      assert_equal [["bob", "hunter2"]], received
    ensure
      sio&.close
      cio&.close
    end
  end

  it "raises when authenticator rejects credentials" do
    Async do
      server_mech = Plain.new(authenticator: ->(_u, _p) { false })
      client_mech = Plain.new(username: "eve", password: "wrong")
      server, client, sio, cio = make_pair(server_mech: server_mech, client_mech: client_mech)

      errors = []
      Barrier do |bar|
        bar.async do
          server.handshake!
        rescue Protocol::ZMTP::Error, EOFError => e
          errors << e
          sio.close rescue nil
        end
        bar.async do
          client.handshake!
        rescue Protocol::ZMTP::Error, EOFError => e
          errors << e
          cio.close rescue nil
        end
      end

      refute_empty errors
    ensure
      sio&.close rescue nil
      cio&.close rescue nil
    end
  end

  it "sends and receives messages after handshake" do
    Async do
      server_mech = Plain.new
      client_mech = Plain.new(username: "alice", password: "pass")
      server, client, sio, cio = make_pair(
        server_mech: server_mech, client_mech: client_mech,
        server_type: "PAIR", client_type: "PAIR",
      )
      Barrier do |bar|
        bar.async { server.handshake! }
        bar.async { client.handshake! }
      end

      Async { client.send_message(["hello plain"]) }
      msg = nil
      Async { msg = server.receive_message }.wait

      assert_equal ["hello plain"], msg
    ensure
      sio&.close
      cio&.close
    end
  end

  it "exchanges identity" do
    Async do
      s1, s2 = UNIXSocket.pair
      sio = IO::Stream::Buffered.wrap(s1)
      cio = IO::Stream::Buffered.wrap(s2)
      server = Connection.new(sio, socket_type: "ROUTER", identity: "server-id",
                              as_server: true, mechanism: Plain.new)
      client = Connection.new(cio, socket_type: "DEALER", identity: "client-id",
                              as_server: false,
                              mechanism: Plain.new(username: "u", password: "p"))

      Barrier do |bar|
        bar.async { server.handshake! }
        bar.async { client.handshake! }
      end

      assert_equal "client-id", server.peer_identity
      assert_equal "server-id", client.peer_identity
    ensure
      sio&.close
      cio&.close
    end
  end
end
