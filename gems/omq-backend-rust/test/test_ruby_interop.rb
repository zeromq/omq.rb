# frozen_string_literal: true

require_relative "test_helper"
require "omq/client_server"
require "omq/radio_dish"
require "omq/scatter_gather"
require "omq/channel"
require "omq/peer"
require "securerandom"
require "tmpdir"

ZSTD_TRANSPORT_AVAILABLE = begin
  require "omq/zstd"
  true
rescue LoadError
  false
end

LZ4_TRANSPORT_AVAILABLE = begin
  require "omq/lz4"
  true
rescue LoadError
  false
end

describe "Rust backend Ruby interop" do
  before do
    @ipc_paths = []
  end


  after do
    @ipc_paths.each do |path|
      File.delete(path)
    rescue Errno::ENOENT
    end
  end


  def bind_port(sock)
    sock.bind("tcp://127.0.0.1:0").port
  end


  def bind_endpoint(sock, transport, label)
    case transport
    when :tcp
      "tcp://127.0.0.1:#{bind_port(sock)}"
    when :ipc
      path = File.join(Dir.tmpdir, "omq-rust-ruby-#{label}-#{$$}-#{SecureRandom.hex(4)}.sock")
      @ipc_paths << path
      sock.bind("ipc://#{path}")
      "ipc://#{path}"
    else
      raise ArgumentError, "unknown transport: #{transport.inspect}"
    end
  end


  def wait_connected(*sockets)
    Async::Task.current.with_timeout(2) do
      sockets.each { |socket| socket.peer_connected.wait }
    end
  end


  def recv(socket)
    Async::Task.current.with_timeout(2) { socket.receive }
  end


  [:tcp, :ipc].each do |transport|
    it "Rust PUSH -> Ruby PULL over #{transport}" do
      Async do
        pull = OMQ::PULL.new(backend: :ruby)
        endpoint = bind_endpoint(pull, transport, "push-rust-ruby")
        push = OMQ::PUSH.new(backend: :rust)
        push.connect(endpoint)
        wait_connected(push)

        5.times { |i| push << "rust-to-ruby-#{i}" }
        assert_equal ["rust-to-ruby-0"], recv(pull)
        assert_equal ["rust-to-ruby-4"], 4.times.map { recv(pull) }.last
      ensure
        push&.close
        pull&.close
      end
    end


    it "Ruby PUSH -> Rust PULL over #{transport}" do
      Async do
        pull = OMQ::PULL.new(backend: :rust)
        endpoint = bind_endpoint(pull, transport, "push-ruby-rust")
        push = OMQ::PUSH.new(backend: :ruby)
        push.connect(endpoint)
        wait_connected(push)

        4.times { |i| push << "ruby-to-rust-#{i}" }
        assert_equal ["ruby-to-rust-0"], recv(pull)
        assert_equal ["ruby-to-rust-3"], 3.times.map { recv(pull) }.last
      ensure
        push&.close
        pull&.close
      end
    end


    it "Rust REQ -> Ruby REP over #{transport}" do
      Async do
        rep = OMQ::REP.new(backend: :ruby)
        endpoint = bind_endpoint(rep, transport, "req-rust-ruby")
        req = OMQ::REQ.new(backend: :rust)
        req.connect(endpoint)
        wait_connected(req)

        3.times do |i|
          req << "ping-#{i}"
          assert_equal ["ping-#{i}"], recv(rep)
          rep << "pong-#{i}"
          assert_equal ["pong-#{i}"], recv(req)
        end
      ensure
        req&.close
        rep&.close
      end
    end


    it "Ruby REQ -> Rust REP over #{transport}" do
      Async do
        rep = OMQ::REP.new(backend: :rust)
        endpoint = bind_endpoint(rep, transport, "req-ruby-rust")
        req = OMQ::REQ.new(backend: :ruby)
        req.connect(endpoint)
        wait_connected(req)

        3.times do |i|
          req << "ping-#{i}"
          assert_equal ["ping-#{i}"], recv(rep)
          rep << "pong-#{i}"
          assert_equal ["pong-#{i}"], recv(req)
        end
      ensure
        req&.close
        rep&.close
      end
    end


    it "Rust PUB -> Ruby SUB over #{transport}" do
      Async do
        pub = OMQ::PUB.new(backend: :rust)
        endpoint = bind_endpoint(pub, transport, "pub-rust-ruby")
        sub = OMQ::SUB.new(backend: :ruby)
        sub.connect(endpoint)
        sub.subscribe("weather.")
        Async::Task.current.with_timeout(2) { pub.subscriber_joined.wait }

        pub.send(["weather.eu", "sunny"])
        pub.send(["news.global", "ignored"])
        pub.send(["weather.us", "rainy"])

        assert_equal ["weather.eu", "sunny"], recv(sub)
        assert_equal ["weather.us", "rainy"], recv(sub)
      ensure
        sub&.close
        pub&.close
      end
    end


    it "Ruby PUB -> Rust SUB over #{transport}" do
      Async do
        pub = OMQ::PUB.new(backend: :ruby)
        endpoint = bind_endpoint(pub, transport, "pub-ruby-rust")
        sub = OMQ::SUB.new(backend: :rust)
        sub.connect(endpoint)
        sub.subscribe("weather.")
        Async::Task.current.with_timeout(2) { pub.subscriber_joined.wait }

        pub.send(["weather.eu", "sunny"])
        pub.send(["news.global", "ignored"])
        pub.send(["weather.us", "rainy"])

        assert_equal ["weather.eu", "sunny"], recv(sub)
        assert_equal ["weather.us", "rainy"], recv(sub)
      ensure
        sub&.close
        pub&.close
      end
    end


    it "Rust ROUTER sees Ruby DEALER identity over #{transport}" do
      Async do
        router = OMQ::ROUTER.new(backend: :rust)
        endpoint = bind_endpoint(router, transport, "router-rust-ruby")
        dealer = OMQ::DEALER.new(backend: :ruby)
        dealer.identity = "worker-7"
        dealer.connect(endpoint)
        wait_connected(dealer)

        dealer << "from-dealer"
        assert_equal ["worker-7", "from-dealer"], recv(router)
      ensure
        dealer&.close
        router&.close
      end
    end


    it "Ruby ROUTER sees Rust DEALER identity over #{transport}" do
      Async do
        router = OMQ::ROUTER.new(backend: :ruby)
        endpoint = bind_endpoint(router, transport, "router-ruby-rust")
        dealer = OMQ::DEALER.new(backend: :rust)
        dealer.identity = "worker-8"
        dealer.connect(endpoint)
        wait_connected(dealer)

        dealer << "from-rust-dealer"
        assert_equal ["worker-8", "from-rust-dealer"], recv(router)
      ensure
        dealer&.close
        router&.close
      end
    end


    it "Rust RADIO -> Ruby DISH over #{transport}" do
      Async do
        radio = OMQ::RADIO.new(backend: :rust)
        endpoint = bind_endpoint(radio, transport, "radio-rust-ruby")
        dish = OMQ::DISH.new(backend: :ruby)
        dish.connect(endpoint)
        dish.join("weather")
        wait_connected(dish)
        Async::Task.current.with_timeout(2) { radio.subscriber_joined.wait }

        radio.publish("sunny", group: "weather")
        radio.publish("ignored", group: "news")
        radio.publish("rainy", group: "weather")

        assert_equal ["sunny"], recv(dish)
        assert_equal ["rainy"], recv(dish)
      ensure
        dish&.close
        radio&.close
      end
    end


    it "Ruby RADIO -> Rust DISH over #{transport}" do
      Async do
        radio = OMQ::RADIO.new(backend: :ruby)
        endpoint = bind_endpoint(radio, transport, "radio-ruby-rust")
        dish = OMQ::DISH.new(backend: :rust)
        dish.connect(endpoint)
        dish.join("weather")
        wait_connected(dish)
        Async::Task.current.with_timeout(2) { radio.subscriber_joined.wait }

        radio.publish("sunny", group: "weather")
        radio.publish("ignored", group: "news")
        radio.publish("rainy", group: "weather")

        assert_equal ["sunny"], recv(dish)
        assert_equal ["rainy"], recv(dish)
      ensure
        dish&.close
        radio&.close
      end
    end
  end


  describe "compression transports" do
    it "Rust PUSH -> Ruby PULL over zstd+tcp" do
      skip "omq/zstd unavailable" unless ZSTD_TRANSPORT_AVAILABLE

      Async do
        pull = OMQ::PULL.new(backend: :ruby)
        endpoint = pull.bind("zstd+tcp://127.0.0.1:0").to_s
        push = OMQ::PUSH.new(backend: :rust)
        push.connect(endpoint, level: 3)
        wait_connected(push)

        payload = ("zstd-rust-ruby-" * 128).b
        push << payload
        assert_equal [payload], recv(pull)
      ensure
        push&.close
        pull&.close
      end
    end


    it "Ruby PUSH -> Rust PULL over zstd+tcp" do
      skip "omq/zstd unavailable" unless ZSTD_TRANSPORT_AVAILABLE

      Async do
        pull = OMQ::PULL.new(backend: :rust)
        endpoint = pull.bind("zstd+tcp://127.0.0.1:0").to_s
        push = OMQ::PUSH.new(backend: :ruby)
        push.connect(endpoint, level: 3)
        wait_connected(push)

        payload = ("zstd-ruby-rust-" * 128).b
        push << payload
        assert_equal [payload], recv(pull)
      ensure
        push&.close
        pull&.close
      end
    end


    it "Rust PUSH -> Ruby PULL over lz4+tcp" do
      skip "omq/lz4 unavailable" unless LZ4_TRANSPORT_AVAILABLE

      Async do
        pull = OMQ::PULL.new(backend: :ruby)
        endpoint = pull.bind("lz4+tcp://127.0.0.1:0").to_s
        push = OMQ::PUSH.new(backend: :rust)
        push.connect(endpoint)
        wait_connected(push)

        payload = ("lz4-rust-ruby-" * 128).b
        push << payload
        assert_equal [payload], recv(pull)
      ensure
        push&.close
        pull&.close
      end
    end


    it "Ruby PUSH -> Rust PULL over lz4+tcp" do
      skip "omq/lz4 unavailable" unless LZ4_TRANSPORT_AVAILABLE

      Async do
        pull = OMQ::PULL.new(backend: :rust)
        endpoint = pull.bind("lz4+tcp://127.0.0.1:0").to_s
        push = OMQ::PUSH.new(backend: :ruby)
        push.connect(endpoint)
        wait_connected(push)

        payload = ("lz4-ruby-rust-" * 128).b
        push << payload
        assert_equal [payload], recv(pull)
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
