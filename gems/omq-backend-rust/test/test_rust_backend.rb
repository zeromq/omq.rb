# frozen_string_literal: true

require_relative "test_helper"
require "omq/client_server"
require "omq/radio_dish"
require "omq/scatter_gather"
require "omq/channel"
require "omq/peer"

describe "Rust backend" do
  def bind_port(sock)
    ep = sock.bind("tcp://127.0.0.1:0")
    ep.port
  end


  describe "receive lifecycle" do
    it "honors recv_timeout before bind or connect" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND, recv_timeout: 0.02)

        assert_raises(IO::TimeoutError) { pull.receive }
      ensure
        pull&.close
      end
    end


    it "close_read wakes a blocked receive with nil" do
      Async do |task|
        pull = OMQ::PULL.new(backend: BACKEND)
        reader = task.async { pull.receive }

        sleep 0.05
        pull.close_read

        assert_nil task.with_timeout(1) { reader.wait }
      ensure
        pull&.close
      end
    end
  end


  describe "PUSH/PULL" do
    it "sends and receives a single message" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "hello"
        assert_equal ["hello"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "sends multipart messages" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push.send(["part1", "part2", "part3"])
        assert_equal ["part1", "part2", "part3"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "handles binary data" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        binary = (0..255).map(&:chr).join.b
        push << binary
        msg = pull.receive
        assert_equal [binary], msg
        assert_equal Encoding::BINARY, msg.first.encoding
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "REQ/REP" do
    it "round-trips request and reply" do
      Async do
        rep = OMQ::REP.new(backend: BACKEND)
        port = bind_port(rep)
        req = OMQ::REQ.new(backend: BACKEND)
        req.connect("tcp://127.0.0.1:#{port}")
        req.peer_connected.wait

        req << "ping"
        assert_equal ["ping"], rep.receive
        rep << "pong"
        assert_equal ["pong"], req.receive
      ensure
        req&.close
        rep&.close
      end
    end
  end


  describe "PUB/SUB" do
    it "delivers to subscriber" do
      Async do
        pub = OMQ::PUB.new(backend: BACKEND)
        port = bind_port(pub)
        sub = OMQ::SUB.new(backend: BACKEND)
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.subscribe("")
        pub.subscriber_joined.wait

        pub << "broadcast"
        assert_equal ["broadcast"], sub.receive
      ensure
        pub&.close
        sub&.close
      end
    end


    it "filters by prefix" do
      Async do
        pub = OMQ::PUB.new(backend: BACKEND)
        port = bind_port(pub)
        sub = OMQ::SUB.new(backend: BACKEND)
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.subscribe("A")
        pub.subscriber_joined.wait

        pub << "Ahit"
        pub << "Bmiss"
        pub << "Ahit2"
        assert_equal ["Ahit"], sub.receive
        assert_equal ["Ahit2"], sub.receive
      ensure
        pub&.close
        sub&.close
      end
    end
  end


  describe "PAIR" do
    it "sends bidirectionally" do
      Async do
        a = OMQ::PAIR.new(backend: BACKEND)
        port = bind_port(a)
        b = OMQ::PAIR.new(backend: BACKEND)
        b.connect("tcp://127.0.0.1:#{port}")
        b.peer_connected.wait

        b << "b->a"
        assert_equal ["b->a"], a.receive
        a << "a->b"
        assert_equal ["a->b"], b.receive
      ensure
        a&.close
        b&.close
      end
    end
  end


  describe "DEALER/ROUTER" do
    it "routes with identity envelope" do
      Async do
        router = OMQ::ROUTER.new(backend: BACKEND)
        port = bind_port(router)
        dealer = OMQ::DEALER.new(backend: BACKEND, identity: "D1")
        dealer.connect("tcp://127.0.0.1:#{port}")
        dealer.peer_connected.wait

        dealer << "request"
        msg = router.receive
        assert_equal 3, msg.size
        identity = msg[0]
        assert_equal "", msg[1]
        assert_equal "request", msg[2]

        router.send([identity, "", "reply"])
        assert_equal ["", "reply"], dealer.receive
      ensure
        dealer&.close
        router&.close
      end
    end
  end


  describe "XPUB/XSUB" do
    it "relays subscriptions" do
      Async do
        xpub = OMQ::XPUB.new(backend: BACKEND)
        port = bind_port(xpub)
        xsub = OMQ::XSUB.new(backend: BACKEND)
        xsub.connect("tcp://127.0.0.1:#{port}")
        xsub.subscribe("")
        xpub.subscriber_joined.wait

        xpub << "hello"
        assert_equal ["hello"], xsub.receive
      ensure
        xpub&.close
        xsub&.close
      end
    end
  end


  describe "CLIENT/SERVER" do
    it "exchanges messages" do
      Async do
        server = OMQ::SERVER.new(backend: BACKEND)
        port = bind_port(server)
        client = OMQ::CLIENT.new(backend: BACKEND)
        client.connect("tcp://127.0.0.1:#{port}")
        client.peer_connected.wait

        client << "hello"
        msg = server.receive
        assert_equal ["hello"], msg[1..]
        server.send(msg)
        assert_equal ["hello"], client.receive
      ensure
        client&.close
        server&.close
      end
    end
  end


  describe "SCATTER/GATHER" do
    it "distributes work" do
      Async do
        gather = OMQ::GATHER.new(backend: BACKEND)
        port = bind_port(gather)
        scatter = OMQ::SCATTER.new(backend: BACKEND)
        scatter.connect("tcp://127.0.0.1:#{port}")
        scatter.peer_connected.wait

        scatter << "work"
        assert_equal ["work"], gather.receive
      ensure
        scatter&.close
        gather&.close
      end
    end
  end


  describe "RADIO/DISH" do
    it "delivers to joined group" do
      Async do
        radio = OMQ::RADIO.new(backend: BACKEND)
        port = bind_port(radio)
        dish = OMQ::DISH.new(backend: BACKEND)
        dish.connect("tcp://127.0.0.1:#{port}")
        dish.join("grp")
        radio.peer_connected.wait
        sleep 0.05

        radio.publish("msg", group: "grp")
        assert_equal ["msg"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end
  end


  describe "CHANNEL" do
    it "sends bidirectionally" do
      Async do
        a = OMQ::CHANNEL.new(backend: BACKEND)
        port = bind_port(a)
        b = OMQ::CHANNEL.new(backend: BACKEND)
        b.connect("tcp://127.0.0.1:#{port}")
        b.peer_connected.wait

        b << "hi"
        assert_equal ["hi"], a.receive
      ensure
        a&.close
        b&.close
      end
    end
  end


  describe "cross-backend interop" do
    it "Rust PUSH -> Ruby PULL" do
      Async do
        pull = OMQ::PULL.new(backend: :ruby)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: :rust)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "rust-to-ruby"
        assert_equal ["rust-to-ruby"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "Ruby PUSH -> Rust PULL" do
      Async do
        pull = OMQ::PULL.new(backend: :rust)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: :ruby)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "ruby-to-rust"
        assert_equal ["ruby-to-rust"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "Rust PUB -> Ruby SUB" do
      Async do
        pub = OMQ::PUB.new(backend: :rust)
        port = bind_port(pub)
        sub = OMQ::SUB.new(backend: :ruby)
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.subscribe("")
        pub.subscriber_joined.wait

        pub << "cross-pubsub"
        assert_equal ["cross-pubsub"], sub.receive
      ensure
        pub&.close
        sub&.close
      end
    end


    it "Ruby REQ -> Rust REP" do
      Async do
        rep = OMQ::REP.new(backend: :rust)
        port = bind_port(rep)
        req = OMQ::REQ.new(backend: :ruby)
        req.connect("tcp://127.0.0.1:#{port}")
        req.peer_connected.wait

        req << "ping"
        assert_equal ["ping"], rep.receive
        rep << "pong"
        assert_equal ["pong"], req.receive
      ensure
        req&.close
        rep&.close
      end
    end
  end


  describe "lifecycle promises" do
    it "resolves peer_connected on handshake" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)

        refute push.peer_connected.resolved?
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait
        assert push.peer_connected.resolved?
      ensure
        push&.close
        pull&.close
      end
    end


    it "resolves subscriber_joined for PUB" do
      Async do
        pub = OMQ::PUB.new(backend: BACKEND)
        port = bind_port(pub)
        sub = OMQ::SUB.new(backend: BACKEND)
        sub.connect("tcp://127.0.0.1:#{port}")
        sub.subscribe("test")
        pub.subscriber_joined.wait
        assert pub.subscriber_joined.resolved?
      ensure
        pub&.close
        sub&.close
      end
    end
  end


  describe "CURVE encryption" do
    it "encrypts end-to-end between Rust sockets" do
      require "nuckle"
      require "protocol/zmtp/mechanism/curve"
      crypto = Nuckle
      server_sec = crypto::PrivateKey.generate
      server_pub = server_sec.public_key
      client_sec = crypto::PrivateKey.generate
      client_pub = client_sec.public_key

      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        pull.mechanism = Protocol::ZMTP::Mechanism::Curve.server(
          public_key: server_pub.to_s, secret_key: server_sec.to_s, crypto: crypto,
        )
        port = bind_port(pull)

        push = OMQ::PUSH.new(backend: BACKEND)
        push.mechanism = Protocol::ZMTP::Mechanism::Curve.client(
          server_key: server_pub.to_s, public_key: client_pub.to_s,
          secret_key: client_sec.to_s, crypto: crypto,
        )
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "encrypted"
        assert_equal ["encrypted"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "interops: Rust CURVE server, Ruby CURVE client" do
      require "nuckle"
      require "protocol/zmtp/mechanism/curve"
      crypto = Nuckle
      server_sec = crypto::PrivateKey.generate
      server_pub = server_sec.public_key
      client_sec = crypto::PrivateKey.generate
      client_pub = client_sec.public_key

      Async do
        pull = OMQ::PULL.new(backend: :rust)
        pull.mechanism = Protocol::ZMTP::Mechanism::Curve.server(
          public_key: server_pub.to_s, secret_key: server_sec.to_s, crypto: crypto,
        )
        port = bind_port(pull)

        push = OMQ::PUSH.new(backend: :ruby)
        push.mechanism = Protocol::ZMTP::Mechanism::Curve.client(
          server_key: server_pub.to_s, public_key: client_pub.to_s,
          secret_key: client_sec.to_s, crypto: crypto,
        )
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        push << "cross-curve"
        assert_equal ["cross-curve"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "IPC transport" do
    it "sends over Unix socket" do
      Async do
        path = "/tmp/omq-rust-test-#{$$}.sock"
        pull = OMQ::PULL.new(backend: BACKEND)
        pull.bind("ipc://#{path}")
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("ipc://#{path}")
        push.peer_connected.wait

        push << "ipc-msg"
        assert_equal ["ipc-msg"], pull.receive
      ensure
        push&.close
        pull&.close
        File.delete(path) rescue nil
      end
    end
  end


  describe "large messages" do
    it "handles 1 MiB payload" do
      Async do
        pull = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(pull)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        big = "X" * (1024 * 1024)
        push << big
        msg = pull.receive
        assert_equal big.bytesize, msg.first.bytesize
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
