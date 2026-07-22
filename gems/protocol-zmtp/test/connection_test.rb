# frozen_string_literal: true

require_relative "test_helper"
require "socket"
require "io/stream"

describe Protocol::ZMTP::Connection do
  Connection = Protocol::ZMTP::Connection

  def make_pair(server_mech: nil, client_mech: nil,
                server_type: "REP", client_type: "REQ")
    s1, s2 = UNIXSocket.pair
    server_io = IO::Stream::Buffered.wrap(s1)
    client_io = IO::Stream::Buffered.wrap(s2)

    server = Connection.new(
      server_io, socket_type: server_type, as_server: true,
      mechanism: server_mech,
    )
    client = Connection.new(
      client_io, socket_type: client_type, as_server: false,
      mechanism: client_mech,
    )

    [server, client, server_io, client_io]
  end

  describe "#handshake!" do
    it "completes NULL handshake between compatible types" do
      Async do
        server, client, sio, cio = make_pair

        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        assert_equal "REP", client.peer_socket_type
        assert_equal "REQ", server.peer_socket_type
        assert_equal "", client.peer_identity
        assert_equal "", server.peer_identity
      ensure
        sio&.close
        cio&.close
      end
    end

    it "exchanges identity" do
      Async do
        server, client, sio, cio = make_pair(
          server_type: "ROUTER", client_type: "DEALER",
        )
        client_io = cio  # need to reach it for mechanism override
        # Recreate with identity
        s1, s2 = UNIXSocket.pair
        sio = IO::Stream::Buffered.wrap(s1)
        cio = IO::Stream::Buffered.wrap(s2)
        server = Connection.new(sio, socket_type: "ROUTER", identity: "server-1", as_server: true)
        client = Connection.new(cio, socket_type: "DEALER", identity: "client-1", as_server: false)

        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        assert_equal "client-1", server.peer_identity
        assert_equal "server-1", client.peer_identity
      ensure
        sio&.close
        cio&.close
      end
    end

    it "records peer ZMTP version from the greeting" do
      Async do
        server, client, sio, cio = make_pair

        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        assert_equal 3, server.peer_major
        assert_equal 1, server.peer_minor
        assert_equal 3, client.peer_major
        assert_equal 1, client.peer_minor
      ensure
        sio&.close
        cio&.close
      end
    end

    it "rejects a ZMTP 2.0 peer after sniffing the signature" do
      Async do
        s1, s2 = UNIXSocket.pair
        sio = IO::Stream::Buffered.wrap(s1)
        cio = IO::Stream::Buffered.wrap(s2)

        server = Connection.new(sio, socket_type: "REP", as_server: true)

        # ZMTP 2.0: 10-byte signature + revision=0x01 + socket-type + ...
        # We only need to deliver enough bytes to let the server read 11
        # and raise; nothing more would ever arrive from a real 2.0 peer.
        zmtp2 = "\xFF".b + ("\x00" * 8) + "\x7F".b + "\x01".b

        error = nil
        Barrier do |bar|
          bar.async do
            server.handshake!
          rescue Protocol::ZMTP::Error => e
            error = e
          end
          bar.async do
            cio.write(zmtp2)
            cio.flush
          end
        end

        refute_nil error
        assert_match(/unsupported ZMTP revision 0x01/, error.message)
      ensure
        sio&.close
        cio&.close
      end
    end

    it "records peer_minor=0 when peer is ZMTP 3.0" do
      Async do
        s1, s2 = UNIXSocket.pair
        sio = IO::Stream::Buffered.wrap(s1)
        cio = IO::Stream::Buffered.wrap(s2)

        server = Connection.new(sio, socket_type: "REP", as_server: true)

        # Hand-craft a ZMTP 3.0 client: identical greeting but minor=0.
        greeting = Protocol::ZMTP::Codec::Greeting.encode(mechanism: "NULL", as_server: false).b
        greeting.setbyte(11, 0)

        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async do
            cio.write(greeting)
            cio.flush
            cio.read_exactly(Protocol::ZMTP::Codec::Greeting::SIZE)
            ready = Protocol::ZMTP::Codec::Command.ready(socket_type: "REQ", identity: "")
            cio.write(ready.to_frame.to_wire)
            cio.flush
            Protocol::ZMTP::Codec::Frame.read_from(cio) # server READY
          end
        end

        assert_equal 3, server.peer_major
        assert_equal 0, server.peer_minor
      ensure
        sio&.close
        cio&.close
      end
    end

    it "rejects incompatible socket types" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PUB", client_type: "REQ")

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

    it "works for all valid socket type pairs" do
      valid_pairs = [
        %w[PAIR PAIR],
        %w[REQ REP],     %w[REQ ROUTER],
        %w[DEALER REP],  %w[DEALER DEALER],
        %w[DEALER ROUTER], %w[ROUTER ROUTER],
        %w[PUB SUB],     %w[PUB XSUB],
        %w[XPUB SUB],    %w[XPUB XSUB],
        %w[PUSH PULL],
      ]

      valid_pairs.each do |type_a, type_b|
        Async do
          s1, s2 = UNIXSocket.pair
          io_a   = IO::Stream::Buffered.wrap(s1)
          io_b   = IO::Stream::Buffered.wrap(s2)
          conn_a = Connection.new(io_a, socket_type: type_a)
          conn_b = Connection.new(io_b, socket_type: type_b)

          Barrier do |bar|
            bar.async { conn_a.handshake! }
            bar.async { conn_b.handshake! }
          end

          assert_equal type_b, conn_a.peer_socket_type, "#{type_a} should see #{type_b}"
          assert_equal type_a, conn_b.peer_socket_type, "#{type_b} should see #{type_a}"
        ensure
          io_a&.close
          io_b&.close
        end
      end
    end
  end

  describe "#send_message / #receive_message" do
    it "sends and receives single-frame messages" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PAIR", client_type: "PAIR")
        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        Async { client.send_message(["hello"]) }
        msg = nil
        Async { msg = server.receive_message }.wait

        assert_equal ["hello"], msg
      ensure
        sio&.close
        cio&.close
      end
    end

    it "sends and receives multi-frame messages" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PAIR", client_type: "PAIR")
        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        Async { client.send_message(["frame1", "frame2", "frame3"]) }
        msg = nil
        Async { msg = server.receive_message }.wait

        assert_equal ["frame1", "frame2", "frame3"], msg
      ensure
        sio&.close
        cio&.close
      end
    end

    it "handles binary data" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PAIR", client_type: "PAIR")
        Barrier do |bar|
          bar.async { server.handshake! }
          bar.async { client.handshake! }
        end

        binary = (0..255).map(&:chr).join.b
        Async { client.send_message([binary]) }
        msg = nil
        Async { msg = server.receive_message }.wait

        assert_equal [binary], msg
      ensure
        sio&.close
        cio&.close
      end
    end
  end

  describe "#write_wire" do
    it "writes pre-encoded bytes readable as frames" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PAIR", client_type: "PAIR")

        Barrier do |barrier|
          barrier.async { server.handshake! }
          barrier.async { client.handshake! }
        end

        wire = Protocol::ZMTP::Codec::Frame.encode_message(["hello", "world"])

        Async do
          client.write_wire(wire)
          client.flush
        end

        msg = nil
        Async { msg = server.receive_message }.wait

        assert_equal ["hello", "world"], msg
      ensure
        sio&.close
        cio&.close
      end
    end
  end

  describe "#encrypted?" do
    it "returns false for NULL mechanism" do
      Async do
        server, client, sio, cio = make_pair(server_type: "PAIR", client_type: "PAIR")

        Barrier do |barrier|
          barrier.async { server.handshake! }
          barrier.async { client.handshake! }
        end

        refute server.encrypted?
        refute client.encrypted?
      ensure
        sio&.close
        cio&.close
      end
    end
  end

  it "tracks heartbeat timestamps" do
    conn = Connection.new(StringIO.new, socket_type: "PAIR")
    assert_nil conn.last_received_at
    conn.touch_heartbeat
    refute_nil conn.last_received_at
    refute conn.heartbeat_expired?(1.0)
  end
end
