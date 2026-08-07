# frozen_string_literal: true

require_relative "test_helper"
require "omq/client_server"
require "omq/radio_dish"
require "omq/scatter_gather"
require "omq/channel"
require "omq/peer"
require "timeout"

describe "Rust backend" do
  def bind_port(sock)
    ep = sock.bind("tcp://127.0.0.1:0")
    ep.port
  end

  describe "socket options" do
    it "materializes options set through public accessors" do
      option_cases = [
        [:send_hwm, 2],
        [:recv_hwm, 2],
        [:linger, 0],
        [:identity, "D1"],
        [:recv_timeout, 0.1],
        [:send_timeout, 0.1],
        [:read_timeout, 0.1],
        [:write_timeout, 0.1],
        [:router_mandatory, true],
        [:reconnect_interval, 0.01],
        [:reconnect_interval, 0.01..0.1],
        [:heartbeat_interval, 0.1],
        [:heartbeat_ttl, 0.1],
        [:heartbeat_timeout, 0.1],
        [:max_message_size, 4096],
        [:conflate, true],
        [:sndbuf, 4096],
        [:rcvbuf, 4096],
        [:on_mute, :drop_newest],
        [:mechanism, Protocol::ZMTP::Mechanism::Null.new],
      ]

      option_cases.each do |name, value|
        dealer = OMQ::DEALER.new(backend: BACKEND)
        dealer.public_send("#{name}=", value)

        assert_equal value, dealer.public_send(name)
        assert_instance_of URI::Generic, dealer.bind("tcp://127.0.0.1:0")
      ensure
        dealer&.close
      end
    end


    it "rejects negative numeric options that map to Rust sizes or durations" do
      invalid_cases = [
        [:linger, -0.1],
        [:reconnect_interval, -0.1],
        [:heartbeat_interval, -0.1],
        [:heartbeat_ttl, -0.1],
        [:heartbeat_timeout, -0.1],
        [:max_message_size, -1],
        [:sndbuf, -1],
        [:rcvbuf, -1],
      ]

      invalid_cases.each do |name, value|
        dealer = OMQ::DEALER.new(backend: BACKEND)
        dealer.public_send("#{name}=", value)

        assert_raises(ArgumentError, "#{name}=#{value.inspect}") do
          dealer.bind("tcp://127.0.0.1:0")
        end
      ensure
        dealer&.close
      end
    end
  end


  describe "receive lifecycle" do
    it "honors recv_timeout before bind or connect" do
      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND, recv_timeout: 0.02)

        assert_raises(IO::TimeoutError) { pull.receive }
      ensure
        pull&.close
      end
    end


    it "uses wait_readable instead of IO.select while waiting" do
      original_select = nil
      verbose         = $VERBOSE
      skip "TruffleRuby fallback uses io-event Select internally" unless OMQ::Reactor.native_fiber_scheduler?

      original_select = IO.method(:select)
      $VERBOSE = nil
      IO.define_singleton_method(:select) do |*|
        raise "IO.select should not be called"
      end
      $VERBOSE = verbose

      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND, recv_timeout: 0.02)

        assert_raises(IO::TimeoutError) { pull.receive }
      ensure
        pull&.close
      end
    ensure
      $VERBOSE = nil
      IO.define_singleton_method(:select, original_select) if original_select
      $VERBOSE = verbose
    end


    it "close_read wakes a blocked receive with nil" do
      run_backend do |task|
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


  describe "TruffleRuby fallback" do
    it "rejects the pure Ruby backend without a native Fiber scheduler" do
      skip unless TRUFFLERUBY_WITHOUT_ASYNC

      err = assert_raises(NotImplementedError) { OMQ::PULL.new(backend: :ruby) }
      assert_equal "Ruby backend requires native Fiber.scheduler; use backend: :rust", err.message
    end


    it "rejects monitor without a native Fiber scheduler" do
      skip unless TRUFFLERUBY_WITHOUT_ASYNC

      pull = OMQ::PULL.new(backend: BACKEND)
      err = assert_raises(NotImplementedError) { pull.monitor { |_| } }
      assert_equal "Socket#monitor requires native Fiber.scheduler", err.message
    ensure
      pull&.close
    end
  end


  describe "PUSH/PULL" do
    it "sends and receives a single message" do
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
        router = OMQ::ROUTER.new(backend: BACKEND)
        port = bind_port(router)
        dealer = OMQ::DEALER.new(backend: BACKEND)
        dealer.identity = "D1"
        dealer.connect("tcp://127.0.0.1:#{port}")
        dealer.peer_connected.wait

        dealer << "request"
        msg = router.receive
        assert_equal 2, msg.size
        identity = msg[0]
        assert_equal "request", msg[1]

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
      run_backend do
        xpub = OMQ::XPUB.new(backend: BACKEND)
        port = bind_port(xpub)
        xsub = OMQ::XSUB.new(backend: BACKEND)
        xsub.connect("tcp://127.0.0.1:#{port}")
        xsub << "\x01".b
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
      run_backend do
        server = OMQ::SERVER.new(backend: BACKEND)
        port = bind_port(server)
        client = OMQ::CLIENT.new(backend: BACKEND)
        client.connect("tcp://127.0.0.1:#{port}")
        client.peer_connected.wait

        client << "hello"
        msg = server.receive
        assert_equal ["hello"], msg[1..]
        server.send_to(msg[0], msg[1])
        assert_equal ["hello"], client.receive
      ensure
        client&.close
        server&.close
      end
    end
  end


  describe "SCATTER/GATHER" do
    it "distributes work" do
      run_backend do
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
      run_backend do
        radio = OMQ::RADIO.new(backend: BACKEND)
        port = bind_port(radio)
        dish = OMQ::DISH.new(backend: BACKEND)
        dish.connect("tcp://127.0.0.1:#{port}")
        dish.join("grp")
        radio.peer_connected.wait
        sleep 0.05

        radio.publish("grp", "msg")
        assert_equal ["grp", "msg"], dish.receive
      ensure
        radio&.close
        dish&.close
      end
    end
  end


  describe "CHANNEL" do
    it "sends bidirectionally" do
      run_backend do
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
    before do
      skip_without_ruby_backend
    end


    it "Rust PUSH -> Ruby PULL" do
      run_backend do
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
      run_backend do
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
      run_backend do
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
      run_backend do
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


  describe "compression transports" do
    it "round-trips over lz4+tcp" do
      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND)
        uri = pull.bind("lz4+tcp://127.0.0.1:0")
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect(uri.to_s)
        push.peer_connected.wait

        payload = ("A" * 4096).b
        push << payload
        assert_equal [payload], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "accepts a static lz4 dictionary" do
      dict = ("event=login user=alice payload=" * 10).b

      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND)
        uri = pull.bind("lz4+tcp://127.0.0.1:0")
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect(uri.to_s, dict: dict)
        push.peer_connected.wait

        payload = (dict + "body").b
        push << payload
        assert_equal [payload], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "ships a static lz4 dictionary to a raw tcp peer" do
      dict = ("event=login user=alice payload=" * 10).b

      run_backend do
        raw = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(raw)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("lz4+tcp://127.0.0.1:#{port}", dict: dict)
        push.peer_connected.wait

        push << (dict + "body").b
        assert_equal "LZ4D".b, raw.receive.first.byteslice(0, 4)
      ensure
        push&.close
        raw&.close
      end
    end


    it "keeps lz4 auto_dict off by default" do
      run_backend do
        raw = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(raw)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("lz4+tcp://127.0.0.1:#{port}")
        push.peer_connected.wait

        120.times { |i| push << json_payload(i) }
        frames = 120.times.map { raw.receive.first }
        refute frames.any? { |frame| frame.start_with?("LZ4D".b) }
      ensure
        push&.close
        raw&.close
      end
    end


    it "enables lz4 auto_dict when requested" do
      run_backend do
        raw = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(raw)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("lz4+tcp://127.0.0.1:#{port}", auto_dict: true)
        push.peer_connected.wait

        140.times { |i| push << json_payload(i) }
        frames = 141.times.map { raw.receive.first }
        assert frames.any? { |frame| frame.start_with?("LZ4D".b) }
      ensure
        push&.close
        raw&.close
      end
    end


    it "rejects lz4 auto_dict trigger because OMQ.rs has a fixed trigger" do
      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND)
        assert_raises(ArgumentError) do
          pull.bind("lz4+tcp://127.0.0.1:0", auto_dict: { trigger: 20 })
        end
      ensure
        pull&.close
      end
    end


    it "round-trips over zstd+tcp with a custom level" do
      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND)
        uri = pull.bind("zstd+tcp://127.0.0.1:0")
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect(uri.to_s, level: 4)
        push.peer_connected.wait

        payload = ("zstd payload " * 512).b
        push << payload
        assert_equal [payload], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "ships a static zstd dictionary to a raw tcp peer" do
      dict = zstd_test_dict

      run_backend do
        raw = OMQ::PULL.new(backend: BACKEND)
        port = bind_port(raw)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect("zstd+tcp://127.0.0.1:#{port}", dict: dict, level: 4)
        push.peer_connected.wait

        push << ("omq-" * 20).b
        assert_equal "\x37\xA4\x30\xEC".b, raw.receive.first.byteslice(0, 4)
      ensure
        push&.close
        raw&.close
      end
    end


    it "rejects an invalid zstd compression level" do
      run_backend do
        pull = OMQ::PULL.new(backend: BACKEND)
        assert_raises(ArgumentError) do
          pull.bind("zstd+tcp://127.0.0.1:0", level: 99)
        end
      ensure
        pull&.close
      end
    end


    it "rejects negative compression size options" do
      invalid_cases = [
        { compression_threshold: -1 },
        { max_recv_dict_size: -1 },
      ]

      invalid_cases.each do |opts|
        pull = OMQ::PULL.new(backend: BACKEND)
        assert_raises(ArgumentError, opts.inspect) do
          pull.bind("zstd+tcp://127.0.0.1:0", **opts)
        end
      ensure
        pull&.close
      end
    end
  end


  def json_payload(i)
    %Q({"event":"login","user":"user_#{i}","region":"us-east-1","status":200}).b
  end


  def zstd_test_dict
    require "zrip"

    trainer = Zrip::DictTrainer.new(2048)
    200.times do
      trainer.add_sample("omq-omq-omq-omq-omq-omq-shared-prefix\n")
    end
    trainer.train.b
  rescue LoadError
    skip "zrip not available"
  end


  describe "lifecycle promises" do
    it "resolves peer_connected on handshake" do
      run_backend do
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
      run_backend do
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

      run_backend do
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
      skip_without_ruby_backend

      require "nuckle"
      require "protocol/zmtp/mechanism/curve"
      crypto = Nuckle
      server_sec = crypto::PrivateKey.generate
      server_pub = server_sec.public_key
      client_sec = crypto::PrivateKey.generate
      client_pub = client_sec.public_key

      run_backend do
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


  describe "inproc transport" do
    it "uses the native Rust inproc endpoint" do
      run_backend do
        endpoint = "inproc://rust-inproc-#{object_id}"
        pull = OMQ::PULL.new(backend: BACKEND)
        pull.bind(endpoint)
        push = OMQ::PUSH.new(backend: BACKEND)
        push.connect(endpoint)
        push.peer_connected.wait

        push << "inproc-msg"
        assert_equal ["inproc-msg"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "IPC transport" do
    it "sends over Unix socket" do
      run_backend do
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


    it "delivers PUB/SUB after forking with a background reactor" do
      skip "fork unavailable" unless Process.respond_to?(:fork)

      n = 6
      path = "/tmp/omq-rust-pubsub-fork-#{$$}.sock"
      endpoint = "ipc://#{path}"
      ready_r, ready_w = IO.pipe
      result_r, result_w = IO.pipe
      children = []
      pub = OMQ::PUB.new(backend: BACKEND)
      pub.linger = 0
      pub.bind(endpoint)

      n.times do |i|
        children << fork do
          ready_r.close
          result_r.close
          child = nil

          begin
            child = OMQ::SUB.new(backend: BACKEND)
            child.linger = 0
            child.read_timeout = 3
            child.connect(endpoint)
            child.subscribe("")
            ready_w.write("R")
            result_w.write("#{i}:#{child.receive.first}\n")
          rescue => error
            result_w.write("#{i}:ERR #{error.class}: #{error.message}\n") rescue nil
          ensure
            child&.close
            ready_w.close rescue nil
            result_w.close rescue nil
            exit! 0
          end
        end
      end

      ready_w.close
      result_w.close
      assert_equal "R" * n, Timeout.timeout(3) { ready_r.read(n) }

      sleep 0.5
      10.times { |i| pub << "msg-#{i}" }

      result = +""
      Timeout.timeout(5) do
        result << result_r.readpartial(1024) while result.lines.size < n
      end

      lines = result.lines
      assert_equal n, lines.size
      lines.each do |line|
        assert_match(/^\d+:msg-\d+$/, line.chomp)
      end
    ensure
      children&.each do |pid|
        Process.kill("KILL", pid) rescue nil
        Process.wait(pid) rescue nil
      end
      pub&.close
      ready_r&.close rescue nil
      ready_w&.close rescue nil
      result_r&.close rescue nil
      result_w&.close rescue nil
      File.delete(path) rescue nil
    end


    it "delivers to forked IPC subscribers using constructor subscriptions" do
      skip "fork unavailable" unless Process.respond_to?(:fork)

      n = 6
      path = "/tmp/omq-rust-pubsub-constructor-fork-#{$$}.sock"
      endpoint = "ipc://#{path}"
      ready_r, ready_w = IO.pipe
      result_r, result_w = IO.pipe
      children = []
      pub = OMQ::PUB.new(backend: BACKEND)
      pub.linger = 0
      pub.bind(endpoint)

      n.times do |i|
        children << fork do
          ready_r.close
          result_r.close
          child = nil

          begin
            child = OMQ::SUB.connect(endpoint, subscribe: "", backend: BACKEND)
            child.linger = 0
            child.read_timeout = 3
            ready_w.write("R")
            result_w.write("#{i}:#{child.receive.first}\n")
          rescue => error
            result_w.write("#{i}:ERR #{error.class}: #{error.message}\n") rescue nil
          ensure
            child&.close
            ready_w.close rescue nil
            result_w.close rescue nil
            exit! 0
          end
        end
      end

      ready_w.close
      result_w.close
      assert_equal "R" * n, Timeout.timeout(3) { ready_r.read(n) }

      10.times { |i| pub << "msg-#{i}" }

      result = +""
      Timeout.timeout(5) do
        result << result_r.readpartial(1024) while result.lines.size < n
      end

      lines = result.lines
      assert_equal n, lines.size
      lines.each do |line|
        assert_match(/^\d+:msg-\d+$/, line.chomp)
      end
    ensure
      children&.each do |pid|
        Process.kill("KILL", pid) rescue nil
        Process.wait(pid) rescue nil
      end
      pub&.close
      ready_r&.close rescue nil
      ready_w&.close rescue nil
      result_r&.close rescue nil
      result_w&.close rescue nil
      File.delete(path) rescue nil
    end
  end


  describe "large messages" do
    it "handles 1 MiB payload" do
      run_backend do
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
