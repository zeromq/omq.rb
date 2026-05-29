# frozen_string_literal: true

require_relative "../test_helper"
require "nuckle"
require "protocol/zmtp/mechanism/curve"

# RbNaCl is optional — only used for cross-backend interop tests
HAVE_RBNACL = begin
  require "rbnacl"
  true
rescue LoadError
  false
end

CurveMech = Protocol::ZMTP::Mechanism::Curve

describe "CURVE encryption (socket-level)" do
  # Default crypto backend — always available, no libsodium needed
  CRYPTO = Nuckle

  def generate_keypair(crypto = CRYPTO)
    secret = crypto::PrivateKey.generate
    [secret.public_key.to_s, secret.to_s]
  end

  def curve_server(pub, sec, crypto: CRYPTO, **opts)
    CurveMech.server(pub, sec, crypto: crypto, **opts)
  end

  def curve_client(pub, sec, server_key:, crypto: CRYPTO)
    CurveMech.client(pub, sec, server_key: server_key, crypto: crypto)
  end

  describe "REQ/REP over TCP" do
    it "works end-to-end" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec)
        port = rep.bind("tcp://127.0.0.1:0").port

        task.async do
          msg = rep.receive
          rep << msg.map(&:upcase)
        end

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.connect("tcp://127.0.0.1:#{port}")

        req << "hello"
        reply = req.receive
        assert_equal ["HELLO"], reply
      ensure
        req&.close
        rep&.close
      end
    end
  end

  describe "PUB/SUB over IPC" do
    it "works end-to-end" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair
      addr = "ipc:///tmp/omq_curve_test_#{$$}.sock"

      Async do |task|
        pub = OMQ::PUB.new
        pub.mechanism = curve_server(server_pub, server_sec)
        pub.bind(addr)

        sub = OMQ::SUB.new
        sub.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        sub.connect(addr)
        sub.subscribe("")
        pub.subscriber_joined.wait

        task.async { pub << "encrypted news" }
        msg = sub.receive
        assert_equal ["encrypted news"], msg
      ensure
        pub&.close
        sub&.close
      end
    end
  end

  describe "Authentication" do
    it "allows a client in the allowed Set" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec, authenticator: Set[client_pub])
        port = rep.bind("tcp://127.0.0.1:0").port

        task.async do
          msg = rep.receive
          rep << msg.map(&:upcase)
        end

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.connect("tcp://127.0.0.1:#{port}")

        req << "authenticated"
        reply = req.receive
        assert_equal ["AUTHENTICATED"], reply
      ensure
        req&.close
        rep&.close
      end
    end

    it "rejects a client not in the allowed Set" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair
      other_pub, _           = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec, authenticator: Set[other_pub])
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.recv_timeout = 0.1
        req.send_timeout = 0.1
        req.connect("tcp://127.0.0.1:#{port}")

        req << "should fail"
        assert_raises(IO::TimeoutError) { req.receive }
      ensure
        req&.close
        rep&.close
      end
    end

    it "works with a callable authenticator" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair
      authenticated_keys = []

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec, authenticator: ->(key) {
          authenticated_keys << key
          true
        })
        port = rep.bind("tcp://127.0.0.1:0").port

        task.async do
          msg = rep.receive
          rep << msg.map(&:upcase)
        end

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.connect("tcp://127.0.0.1:#{port}")

        req << "hello"
        reply = req.receive
        assert_equal ["HELLO"], reply
        assert_equal [client_pub], authenticated_keys
      ensure
        req&.close
        rep&.close
      end
    end

    it "rejects when callable returns false" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec, authenticator: ->(_) { false })
        port = rep.bind("tcp://127.0.0.1:0").port

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.recv_timeout = 0.1
        req.send_timeout = 0.1
        req.connect("tcp://127.0.0.1:#{port}")

        req << "should fail"
        assert_raises(IO::TimeoutError) { req.receive }
      ensure
        req&.close
        rep&.close
      end
    end
  end

  describe "Multiple clients" do
    it "supports multiple clients to one server" do
      server_pub, server_sec = generate_keypair
      c1_pub, c1_sec = generate_keypair
      c2_pub, c2_sec = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec)
        port = rep.bind("tcp://127.0.0.1:0").port

        task.async do
          2.times do
            msg = rep.receive
            rep << msg.map(&:upcase)
          end
        end

        req1 = OMQ::REQ.new
        req1.mechanism = curve_client(c1_pub, c1_sec, server_key: server_pub)
        req1.connect("tcp://127.0.0.1:#{port}")

        req2 = OMQ::REQ.new
        req2.mechanism = curve_client(c2_pub, c2_sec, server_key: server_pub)
        req2.connect("tcp://127.0.0.1:#{port}")

        req1 << "from client 1"
        assert_equal ["FROM CLIENT 1"], req1.receive

        req2 << "from client 2"
        assert_equal ["FROM CLIENT 2"], req2.receive
      ensure
        req1&.close
        req2&.close
        rep&.close
      end
    end
  end

  describe "Reconnect" do
    it "reconnects after server restart" do
      server_pub, server_sec = generate_keypair
      client_pub, client_sec = generate_keypair

      Async do |task|
        rep = OMQ::REP.new
        rep.mechanism = curve_server(server_pub, server_sec)
        port = rep.bind("tcp://127.0.0.1:0").port

        task.async do
          msg = rep.receive
          rep << msg.map(&:upcase)
        end

        req = OMQ::REQ.new
        req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub)
        req.reconnect_interval = RECONNECT_INTERVAL
        req.connect("tcp://127.0.0.1:#{port}")

        req << "first"
        assert_equal ["FIRST"], req.receive

        rep.close
        sleep 0.02

        rep2 = OMQ::REP.new
        rep2.mechanism = curve_server(server_pub, server_sec)
        rep2.bind("tcp://127.0.0.1:#{port}")

        task.async do
          msg = rep2.receive
          rep2 << msg.map(&:upcase)
        end

        wait_connected(req, rep2)
        req << "second"
        assert_equal ["SECOND"], req.receive
      ensure
        req&.close
        rep&.close rescue nil
        rep2&.close rescue nil
      end
    end
  end

  if HAVE_RBNACL
    describe "Cross-backend interop" do
      it "RbNaCl server, Nuckle client" do
        server_pub, server_sec = generate_keypair(RbNaCl)
        client_pub, client_sec = generate_keypair(Nuckle)

        Async do |task|
          rep = OMQ::REP.new
          rep.mechanism = curve_server(server_pub, server_sec, crypto: RbNaCl)
          port = rep.bind("tcp://127.0.0.1:0").port

          task.async do
            msg = rep.receive
            rep << msg.map(&:upcase)
          end

          req = OMQ::REQ.new
          req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub, crypto: Nuckle)
          req.connect("tcp://127.0.0.1:#{port}")

          req << "nuckle to rbnacl"
          reply = req.receive
          assert_equal ["NUCKLE TO RBNACL"], reply
        ensure
          req&.close
          rep&.close
        end
      end

      it "Nuckle server, RbNaCl client" do
        server_pub, server_sec = generate_keypair(Nuckle)
        client_pub, client_sec = generate_keypair(RbNaCl)

        Async do |task|
          rep = OMQ::REP.new
          rep.mechanism = curve_server(server_pub, server_sec, crypto: Nuckle)
          port = rep.bind("tcp://127.0.0.1:0").port

          task.async do
            msg = rep.receive
            rep << msg.map(&:upcase)
          end

          req = OMQ::REQ.new
          req.mechanism = curve_client(client_pub, client_sec, server_key: server_pub, crypto: RbNaCl)
          req.connect("tcp://127.0.0.1:#{port}")

          req << "rbnacl to nuckle"
          reply = req.receive
          assert_equal ["RBNACL TO NUCKLE"], reply
        ensure
          req&.close
          rep&.close
        end
      end

      it "PUB/SUB with SUBSCRIBE command frame (RbNaCl server, Nuckle client)" do
        server_pub, server_sec = generate_keypair(RbNaCl)
        client_pub, client_sec = generate_keypair(Nuckle)

        Async do
          pub = OMQ::PUB.new
          pub.mechanism = curve_server(server_pub, server_sec, crypto: RbNaCl)
          port = pub.bind("tcp://127.0.0.1:0").port

          sub = OMQ::SUB.new
          sub.mechanism = curve_client(client_pub, client_sec, server_key: server_pub, crypto: Nuckle)
          sub.connect("tcp://127.0.0.1:#{port}")
          sub.subscribe("news")
          pub.subscriber_joined.wait

          pub << "news: encrypted cross-backend"
          msg = sub.receive
          assert_equal ["news: encrypted cross-backend"], msg
        ensure
          sub&.close
          pub&.close
        end
      end

      it "PUB/SUB with SUBSCRIBE command frame (Nuckle server, RbNaCl client)" do
        server_pub, server_sec = generate_keypair(Nuckle)
        client_pub, client_sec = generate_keypair(RbNaCl)

        Async do
          pub = OMQ::PUB.new
          pub.mechanism = curve_server(server_pub, server_sec, crypto: Nuckle)
          port = pub.bind("tcp://127.0.0.1:0").port

          sub = OMQ::SUB.new
          sub.mechanism = curve_client(client_pub, client_sec, server_key: server_pub, crypto: RbNaCl)
          sub.connect("tcp://127.0.0.1:#{port}")
          sub.subscribe("news")
          pub.subscriber_joined.wait

          pub << "news: encrypted cross-backend"
          msg = sub.receive
          assert_equal ["news: encrypted cross-backend"], msg
        ensure
          sub&.close
          pub&.close
        end
      end
    end
  end
end
