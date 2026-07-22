# frozen_string_literal: true

require_relative "../test_helper"
require "protocol/zmtp/mechanism/curve"
require "nuckle"

HAVE_RBNACL = begin
  require "rbnacl"
  true
rescue LoadError
  false
end

describe Protocol::ZMTP::Mechanism::Curve do
  Curve = Protocol::ZMTP::Mechanism::Curve

  def generate_keypair(crypto)
    sk = crypto::PrivateKey.generate
    [sk.public_key.to_s, sk.to_s]
  end

  def make_curve_pair(server_crypto:, client_crypto:, authenticator: nil)
    server_pub, server_sec = generate_keypair(server_crypto)
    client_pub, client_sec = generate_keypair(client_crypto)

    s1, s2 = UNIXSocket.pair
    server_io = IO::Stream::Buffered.wrap(s1)
    client_io = IO::Stream::Buffered.wrap(s2)

    server_mech = Curve.server(
      public_key: server_pub, secret_key: server_sec,
      crypto: server_crypto, authenticator: authenticator,
    )
    client_mech = Curve.client(
      server_key: server_pub, crypto: client_crypto,
      public_key: client_pub, secret_key: client_sec,
    )

    server = Protocol::ZMTP::Connection.new(
      server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
    )
    client = Protocol::ZMTP::Connection.new(
      client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
    )

    [server, client, server_io, client_io]
  end

  backends = [Nuckle]
  backends << RbNaCl if HAVE_RBNACL

  backends.each do |crypto|
    describe "with #{crypto}" do
      it "completes handshake and exchanges messages" do
        Async do
          server, client, sio, cio = make_curve_pair(server_crypto: crypto, client_crypto: crypto)

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          assert_equal "PAIR", client.peer_socket_type
          assert_equal "PAIR", server.peer_socket_type

          Async { client.send_message(["encrypted hello"]) }
          msg = nil
          Async { msg = server.receive_message }.wait
          assert_equal ["encrypted hello"], msg

          Async { server.send_message(["encrypted reply"]) }
          msg2 = nil
          Async { msg2 = client.receive_message }.wait
          assert_equal ["encrypted reply"], msg2
        ensure
          sio&.close
          cio&.close
        end
      end

      it "uses 0x02 for COMMAND in the encrypted inner flags byte" do
        Async do
          server, client, sio, cio = make_curve_pair(server_crypto: crypto, client_crypto: crypto)

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          client_mech = client.instance_variable_get(:@mechanism)
          server_mech = server.instance_variable_get(:@mechanism)

          wire = client_mech.encrypt("test".b, command: true)

          header_size = (wire.getbyte(0) & 0x02) != 0 ? 9 : 2
          msg_body    = wire.byteslice(header_size..)
          short_nonce = msg_body.byteslice(8, 8)
          ciphertext  = msg_body.byteslice(16..)

          session_box    = server_mech.instance_variable_get(:@session_box)
          recv_nonce_buf = server_mech.instance_variable_get(:@recv_nonce_buf).dup
          recv_nonce_buf[16, 8] = short_nonce
          plaintext = session_box.decrypt(recv_nonce_buf, ciphertext)

          inner_flags = plaintext.getbyte(0)
          assert_equal 0x02, inner_flags,
            "COMMAND must be bit 0x02 in encrypted inner byte, got 0x%02x" % inner_flags
        ensure
          sio&.close
          cio&.close
        end
      end

      it "preserves MORE and COMMAND flags through encrypt/decrypt round-trip" do
        Async do
          server, client, sio, cio = make_curve_pair(server_crypto: crypto, client_crypto: crypto)

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          client_mech = client.instance_variable_get(:@mechanism)
          server_mech = server.instance_variable_get(:@mechanism)

          [
            [false, false],
            [true,  false],
            [false, true],
            [true,  true],
          ].each do |more, command|
            wire        = client_mech.encrypt("body".b, more: more, command: command)
            header_size = (wire.getbyte(0) & 0x02) != 0 ? 9 : 2
            body        = wire.byteslice(header_size..)
            frame       = Protocol::ZMTP::Codec::Frame.new(body, command: true)
            decrypted   = server_mech.decrypt(frame)

            assert_equal more, decrypted.more?,
              "more flag mismatch for more=#{more}, command=#{command}"

            assert_equal command, decrypted.command?,
              "command flag mismatch for more=#{more}, command=#{command}"
          end
        ensure
          sio&.close
          cio&.close
        end
      end

      it "is encrypted" do
        pub, sec = generate_keypair(crypto)
        mech = Curve.server(public_key: pub, secret_key: sec, crypto: crypto)
        assert mech.encrypted?
      end

      it "rejects wrong server key" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)
          wrong_pub, _           = generate_keypair(crypto)

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(public_key: server_pub, secret_key: server_sec, crypto: crypto)
          client_mech = Curve.client(
            server_key: wrong_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          errors = []
          Barrier do |bar|
            bar.async do
              server.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              server_io.close rescue nil
            end
            bar.async do
              client.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              client_io.close rescue nil
            end
          end

          refute_empty errors
        ensure
          server_io&.close rescue nil
          client_io&.close rescue nil
        end
      end

      it "raises on invalid key length" do
        assert_raises(ArgumentError) do
          Curve.server(public_key: "short", secret_key: "short", crypto: crypto)
        end
      end

      it "raises on nil keys" do
        assert_raises(ArgumentError) do
          Curve.server(public_key: nil, secret_key: nil, crypto: crypto)
        end
      end
    end
  end

  backends.each do |crypto|
    describe "auto-generated client keys with #{crypto}" do
      it "completes handshake and exchanges messages" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(public_key: server_pub, secret_key: server_sec, crypto: crypto)
          client_mech = Curve.client(server_key: server_pub, crypto: crypto)

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          assert_equal "PAIR", client.peer_socket_type
          assert_equal "PAIR", server.peer_socket_type

          Barrier do |bar|
            bar.async { client.send_message(["auto-key hello"]) }
            bar.async do
              assert_equal ["auto-key hello"], server.receive_message
            end
          end

          Barrier do |bar|
            bar.async { server.send_message(["auto-key reply"]) }
            bar.async do
              assert_equal ["auto-key reply"], client.receive_message
            end
          end
        ensure
          s1&.close
          s2&.close
        end
      end

      it "rejects auto-generated client with wrong server key" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          wrong_pub, _ = generate_keypair(crypto)

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(public_key: server_pub, secret_key: server_sec, crypto: crypto)
          client_mech = Curve.client(server_key: wrong_pub, crypto: crypto)

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          errors = []
          Barrier do |bar|
            bar.async do
              server.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              server_io.close rescue nil
            end
            bar.async do
              client.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              client_io.close rescue nil
            end
          end

          refute_empty errors
        ensure
          s1&.close
          s2&.close
        end
      end
    end
  end

  backends.each do |crypto|
    describe "authenticator with #{crypto}" do
      it "passes a PeerInfo with the client's PublicKey to the authenticator" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)

          received_peer = nil
          authenticator = lambda do |peer|
            received_peer = peer
            true
          end

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(
            public_key: server_pub, secret_key: server_sec,
            crypto: crypto, authenticator: authenticator,
          )
          client_mech = Curve.client(
            server_key: server_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          assert_instance_of Protocol::ZMTP::PeerInfo, received_peer
          assert_instance_of crypto::PublicKey, received_peer.public_key
          assert_equal client_pub, received_peer.public_key.to_s
        ensure
          s1&.close
          s2&.close
        end
      end

      it "accepts when authenticator returns true" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(
            public_key: server_pub, secret_key: server_sec,
            crypto: crypto, authenticator: ->(_) { true },
          )
          client_mech = Curve.client(
            server_key: server_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          assert_equal "PAIR", client.peer_socket_type
        ensure
          s1&.close
          s2&.close
        end
      end

      it "rejects when authenticator returns false" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)

          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(
            public_key: server_pub, secret_key: server_sec,
            crypto: crypto, authenticator: ->(_) { false },
          )
          client_mech = Curve.client(
            server_key: server_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          errors = []
          Barrier do |bar|
            bar.async do
              server.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              server_io.close rescue nil
            end
            bar.async do
              client.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              client_io.close rescue nil
            end
          end

          refute_empty errors
          assert errors.any? { |e| e.message.include?("not authorized") }
        ensure
          s1&.close
          s2&.close
        end
      end

      it "accepts when key is in the allowed set" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)

          allowed_keys = Set.new([client_pub])
          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(
            public_key: server_pub, secret_key: server_sec,
            crypto: crypto,
            authenticator: ->(peer) { allowed_keys.include?(peer.public_key.to_s) },
          )
          client_mech = Curve.client(
            server_key: server_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          assert_equal "PAIR", client.peer_socket_type
        ensure
          s1&.close
          s2&.close
        end
      end

      it "rejects when key is not in the allowed set" do
        Async do
          server_pub, server_sec = generate_keypair(crypto)
          client_pub, client_sec = generate_keypair(crypto)

          allowed_keys = Set.new  # empty
          s1, s2 = UNIXSocket.pair
          server_io = IO::Stream::Buffered.wrap(s1)
          client_io = IO::Stream::Buffered.wrap(s2)

          server_mech = Curve.server(
            public_key: server_pub, secret_key: server_sec,
            crypto: crypto,
            authenticator: ->(peer) { allowed_keys.include?(peer.public_key.to_s) },
          )
          client_mech = Curve.client(
            server_key: server_pub, crypto: crypto,
            public_key: client_pub, secret_key: client_sec,
          )

          server = Protocol::ZMTP::Connection.new(
            server_io, socket_type: "PAIR", as_server: true, mechanism: server_mech,
          )
          client = Protocol::ZMTP::Connection.new(
            client_io, socket_type: "PAIR", as_server: false, mechanism: client_mech,
          )

          errors = []
          Barrier do |bar|
            bar.async do
              server.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              server_io.close rescue nil
            end
            bar.async do
              client.handshake!
            rescue Protocol::ZMTP::Error, EOFError => e
              errors << e
              client_io.close rescue nil
            end
          end

          refute_empty errors
          assert errors.any? { |e| e.message.include?("not authorized") }
        ensure
          s1&.close
          s2&.close
        end
      end
    end
  end

  if HAVE_RBNACL
    describe "interop: RbNaCl server, Nuckle client" do
      it "completes handshake and exchanges messages" do
        Async do
          server, client, sio, cio = make_curve_pair(server_crypto: RbNaCl, client_crypto: Nuckle)

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          Async { client.send_message(["nuckle->rbnacl"]) }
          msg = nil
          Async { msg = server.receive_message }.wait
          assert_equal ["nuckle->rbnacl"], msg
        ensure
          sio&.close
          cio&.close
        end
      end
    end

    describe "interop: Nuckle server, RbNaCl client" do
      it "completes handshake and exchanges messages" do
        Async do
          server, client, sio, cio = make_curve_pair(server_crypto: Nuckle, client_crypto: RbNaCl)

          Barrier do |bar|
            bar.async { server.handshake! }
            bar.async { client.handshake! }
          end

          Async { client.send_message(["rbnacl->nuckle"]) }
          msg = nil
          Async { msg = server.receive_message }.wait
          assert_equal ["rbnacl->nuckle"], msg
        ensure
          sio&.close
          cio&.close
        end
      end
    end
  end
end
