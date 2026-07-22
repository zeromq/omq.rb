# frozen_string_literal: true

require_relative "../test_helper"

describe "OMQ::Ractor" do
  before { OMQ::Transport::Inproc.reset! }

  # ── Raw mode (serialize: false) ────────────────────────────

  it "raw PULL → PUSH pipeline over inproc" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-raw-pipe")
      push = OMQ::PUSH.bind("ruby://r-raw-pipe-out")

      worker = OMQ::Ractor.new(pull, push, serialize: false) do |omq|
        p_in, p_out = omq.sockets
        3.times do
          msg = p_in.receive
          p_out << [msg.first.upcase]
        end
      end

      sender   = OMQ::PUSH.connect("ruby://r-raw-pipe")
      receiver = OMQ::PULL.connect("ruby://r-raw-pipe-out")
      wait_connected(sender, receiver)

      3.times { |i| sender << "msg-#{i}" }
      results = 3.times.map { receiver.receive.first }

      worker.join
      assert_equal %w[MSG-0 MSG-1 MSG-2], results
    ensure
      [sender, receiver, pull, push].compact.each(&:close)
    end
  end


  it "raw PAIR bidirectional" do
    Async do
      pair_a = OMQ::PAIR.bind("ruby://r-raw-pair")

      worker = OMQ::Ractor.new(pair_a, serialize: false) do |omq|
        p = omq.sockets.first
        3.times do
          msg = p.receive
          p << [msg.first.reverse]
        end
      end

      pair_b = OMQ::PAIR.connect("ruby://r-raw-pair")
      wait_connected(pair_b)

      results = 3.times.map do |i|
        pair_b << "test-#{i}"
        pair_b.receive.first
      end

      worker.join
      assert_equal %w[0-tset 1-tset 2-tset], results
    ensure
      [pair_a, pair_b].compact.each(&:close)
    end
  end


  it "raw REQ/REP round-trip" do
    Async do
      rep = OMQ::REP.bind("ruby://r-raw-reqrep")

      worker = OMQ::Ractor.new(rep, serialize: false) do |omq|
        p = omq.sockets.first
        3.times do
          msg = p.receive
          p << [msg.first.upcase]
        end
      end

      req = OMQ::REQ.connect("ruby://r-raw-reqrep")
      wait_connected(req)

      results = 3.times.map do |i|
        req << "req-#{i}"
        req.receive.first
      end

      worker.join
      assert_equal %w[REQ-0 REQ-1 REQ-2], results
    ensure
      [req, rep].compact.each(&:close)
    end
  end


  it "raw multiplexing with Ractor.select" do
    Async do
      pull_a = OMQ::PULL.bind("ruby://r-raw-mux-a")
      pull_b = OMQ::PULL.bind("ruby://r-raw-mux-b")
      push   = OMQ::PUSH.bind("ruby://r-raw-mux-out")

      worker = OMQ::Ractor.new(pull_a, pull_b, push, serialize: false) do |omq|
        a, b, out = omq.sockets
        4.times do
          source, msg = Ractor.select(a.to_port, b.to_port)
          label = source == a.to_port ? "A" : "B"
          out << ["#{label}:#{msg.first}"]
        end
      end

      sender_a = OMQ::PUSH.connect("ruby://r-raw-mux-a")
      sender_b = OMQ::PUSH.connect("ruby://r-raw-mux-b")
      receiver = OMQ::PULL.connect("ruby://r-raw-mux-out")
      wait_connected(sender_a, sender_b, receiver)

      sender_a << "alpha"
      sender_b << "beta"
      sender_a << "gamma"
      sender_b << "delta"

      results = 4.times.map { receiver.receive.first }
      worker.join

      assert_includes results, "A:alpha"
      assert_includes results, "B:beta"
    ensure
      [sender_a, sender_b, receiver, pull_a, pull_b, push].compact.each(&:close)
    end
  end


  it "SocketSet#socket_for maps port back to proxy" do
    Async do
      pull_a = OMQ::PULL.bind("ruby://r-sockfor-a")
      pull_b = OMQ::PULL.bind("ruby://r-sockfor-b")
      push   = OMQ::PUSH.bind("ruby://r-sockfor-out")

      worker = OMQ::Ractor.new(pull_a, pull_b, push, serialize: false) do |omq|
        sockets = omq.sockets
        a, b, out = sockets
        4.times do
          port, msg = Ractor.select(a.to_port, b.to_port)
          source = sockets.socket_for(port)
          label  = source.equal?(a) ? "A" : "B"
          out << ["#{label}:#{msg.first}"]
        end
      end

      sender_a = OMQ::PUSH.connect("ruby://r-sockfor-a")
      sender_b = OMQ::PUSH.connect("ruby://r-sockfor-b")
      receiver = OMQ::PULL.connect("ruby://r-sockfor-out")
      wait_connected(sender_a, sender_b, receiver)

      sender_a << "one"
      sender_b << "two"
      sender_a << "three"
      sender_b << "four"

      results = 4.times.map { receiver.receive.first }
      worker.join

      assert_includes results, "A:one"
      assert_includes results, "B:two"
      assert_includes results, "A:three"
      assert_includes results, "B:four"
    ensure
      [sender_a, sender_b, receiver, pull_a, pull_b, push].compact.each(&:close)
    end
  end


  # ── Serialization (inproc, make_shareable) ─────────────────

  it "inproc serialization sends Ruby objects via make_shareable" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-ser-inproc")
      push = OMQ::PUSH.bind("ruby://r-ser-inproc-out")

      worker = OMQ::Ractor.new(pull, push) do |omq|
        p_in, p_out = omq.sockets
        3.times do
          msg = p_in.receive
          p_out << { result: msg.upcase }
        end
      end

      sender   = OMQ::PUSH.connect("ruby://r-ser-inproc")
      receiver = OMQ::PULL.connect("ruby://r-ser-inproc-out")
      wait_connected(sender, receiver)

      3.times { |i| sender << "hello-#{i}" }
      results = 3.times.map { receiver.receive }

      worker.join
      # Regular receiver gets the raw shareable message: [obj] wrapped in array
      assert_equal [[{ result: "HELLO-0" }], [{ result: "HELLO-1" }], [{ result: "HELLO-2" }]], results
    ensure
      [sender, receiver, pull, push].compact.each(&:close)
    end
  end


  # ── Serialization (IPC, Marshal) end-to-end ────────────────

  it "IPC end-to-end Marshal between two Ractors" do
    Async do
      pull = OMQ::PULL.bind("ipc://@omq-r-e2e")

      consumer = OMQ::Ractor.new(pull) do |omq|
        p_in = omq.sockets.first
        3.times.map { p_in.receive }
      end

      push = OMQ::PUSH.connect("ipc://@omq-r-e2e")

      producer = OMQ::Ractor.new(push) do |omq|
        p_out = omq.sockets.first
        3.times { |i| p_out << { num: i, text: "hello" } }
      end

      results = consumer.value
      producer.join

      assert_equal 3, results.size
      results.each_with_index do |r, i|
        assert_equal({ num: i, text: "hello" }, r)
      end
    ensure
      [pull, push].compact.each(&:close)
    end
  end


  # ── PUB/SUB with topic-aware serialization ─────────────────

  it "PUB/SUB with #<< sends to all subscribers" do
    Async do
      pub = OMQ::PUB.bind("ruby://r-pubsub-all")

      worker = OMQ::Ractor.new(pub) do |omq|
        pub_p = omq.sockets.first
        sleep 0.05
        3.times { |i| pub_p << { n: i } }
      end

      sub = OMQ::SUB.connect("ruby://r-pubsub-all")
      sub.subscribe("")

      results = 3.times.map { sub.receive }
      worker.join

      # Receiver gets [topic, payload] — topic is "" for << messages
      assert_equal 3, results.size
      results.each_with_index do |r, i|
        assert_equal "", r.first
        assert_equal({ n: i }, r.last)
      end
    ensure
      [pub, sub].compact.each(&:close)
    end
  end


  it "PUB/SUB with #publish and topic prefix filtering" do
    Async do
      pub = OMQ::PUB.bind("ruby://r-pubsub-filter")

      worker = OMQ::Ractor.new(pub) do |omq|
        pub_p = omq.sockets.first
        sleep 0.05
        pub_p.publish({ price: 100 }, topic: "prices.AAPL")
        pub_p.publish({ temp: 72 },   topic: "weather.NYC")
        pub_p.publish({ price: 200 }, topic: "prices.GOOG")
        pub_p.publish({ temp: 65 },   topic: "weather.SF")
      end

      sub = OMQ::SUB.connect("ruby://r-pubsub-filter")
      sub.subscribe("prices.")

      results = 2.times.map { sub.receive }
      worker.join

      topics   = results.map(&:first)
      payloads = results.map(&:last)

      assert_equal ["prices.AAPL", "prices.GOOG"], topics
      assert_equal [{ price: 100 }, { price: 200 }], payloads
    ensure
      [pub, sub].compact.each(&:close)
    end
  end


  it "SUB proxy #receive strips topic, #receive_with_topic returns both" do
    Async do
      pub = OMQ::PUB.bind("ruby://r-sub-proxy")
      sub = OMQ::SUB.connect("ruby://r-sub-proxy")
      sub.subscribe("")

      # Regular PUB sends topic-prefixed messages
      sleep 0.05
      pub << ["prices.AAPL", "100"]
      pub << ["weather.NYC", "72"]

      worker = OMQ::Ractor.new(sub) do |omq|
        sub_p = omq.sockets.first
        # #receive strips topic
        msg1 = sub_p.receive
        # #receive_with_topic returns [topic, payload]
        topic2, msg2 = sub_p.receive_with_topic
        [msg1, topic2, msg2]
      end

      result = worker.value
      assert_equal "100", result[0]
      assert_equal "weather.NYC", result[1]
      assert_equal "72", result[2]
    ensure
      [pub, sub].compact.each(&:close)
    end
  end


  # ── Worker return value / read-only / write-only ───────────

  it "worker return value via #value" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-value")

      worker = OMQ::Ractor.new(pull, serialize: false) do |omq|
        p = omq.sockets.first
        3.times.map { p.receive.first }
      end

      push = OMQ::PUSH.connect("ruby://r-value")
      wait_connected(push)

      3.times { |i| push << "v-#{i}" }

      result = worker.value
      assert_equal %w[v-0 v-1 v-2], result
    ensure
      [push, pull].compact.each(&:close)
    end
  end


  it "write-only socket" do
    Async do
      push = OMQ::PUSH.bind("ruby://r-write-only")

      worker = OMQ::Ractor.new(push) do |omq|
        p = omq.sockets.first
        3.times { |i| p << { n: i } }
      end

      receiver = OMQ::PULL.connect("ruby://r-write-only")
      wait_connected(receiver)

      results = 3.times.map { receiver.receive }
      worker.join
      # Regular receiver gets raw shareable message: [obj] wrapped in array
      assert_equal [[{ n: 0 }], [{ n: 1 }], [{ n: 2 }]], results
    ensure
      [receiver, push].compact.each(&:close)
    end
  end


  it "read-only socket" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-read-only")

      worker = OMQ::Ractor.new(pull) do |omq|
        p = omq.sockets.first
        3.times.map { p.receive }
      end

      sender = OMQ::PUSH.connect("ruby://r-read-only")
      wait_connected(sender)

      3.times { |i| sender << "read-#{i}" }

      result = worker.value
      assert_equal %w[read-0 read-1 read-2], result
    ensure
      [sender, pull].compact.each(&:close)
    end
  end


  # ── Context#data ───────────────────────────────────────────

  it "passes shareable data into the worker via data:" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-data")

      config = { multiplier: 10, prefix: "out" }
      worker = OMQ::Ractor.new(pull, serialize: false, data: config) do |omq|
        sockets = omq.sockets
        p = sockets.first
        d = omq.data
        3.times.map { "#{d[:prefix]}-#{p.receive.first.to_i * d[:multiplier]}" }
      end

      push = OMQ::PUSH.connect("ruby://r-data")
      wait_connected(push)

      3.times { |i| push << i.to_s }

      result = worker.value
      assert_equal %w[out-0 out-10 out-20], result
    ensure
      [push, pull].compact.each(&:close)
    end
  end


  it "data defaults to nil when not provided" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-data-nil")

      worker = OMQ::Ractor.new(pull, serialize: false) do |omq|
        omq.sockets
        omq.data
      end

      push = OMQ::PUSH.connect("ruby://r-data-nil")
      wait_connected(push)

      worker.close
      result = worker.value
      assert_nil result
    ensure
      [push, pull].compact.each(&:close)
    end
  end


  # ── Error handling ─────────────────────────────────────────

  it "handshake timeout when worker doesn't call omq.sockets" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-no-handshake")

      error = assert_raises(ArgumentError) do
        OMQ::Ractor.new(pull) do |omq|
          sleep 1  # doesn't call omq.sockets
        end
      end
      assert_match(/omq\.sockets/, error.message)
    ensure
      pull&.close
    end
  end


  it "proxy.receive returns nil on close, then raises SocketClosedError" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-close-signal")

      worker = OMQ::Ractor.new(pull, serialize: false) do |omq|
        p = omq.sockets.first
        received    = []
        got_nil     = false
        got_error   = false
        error_class = nil

        loop do
          msg = p.receive
          if msg.nil?
            got_nil = true
            # Second call should raise SocketClosedError
            begin
              p.receive
            rescue => e
              got_error   = true
              error_class = e.class.name
            end
            break
          end
          received << msg.first
        end

        { received: received, got_nil: got_nil, got_error: got_error, error_class: error_class }
      end

      push = OMQ::PUSH.connect("ruby://r-close-signal")
      wait_connected(push)

      push << "one"
      push << "two"
      sleep 0.01

      worker.close

      result = worker.value
      assert_equal %w[one two], result[:received]
      assert_equal true, result[:got_nil]
      assert_equal true, result[:got_error]
      assert_equal "OMQ::Ractor::SocketClosedError", result[:error_class]
    ensure
      push&.close
      pull&.close
    end
  end


  it "raises ArgumentError without sockets" do
    assert_raises(ArgumentError) { OMQ::Ractor.new { } }
  end


  it "raises ArgumentError without block" do
    Async do
      pull = OMQ::PULL.bind("ruby://r-noblock")
      assert_raises(ArgumentError) { OMQ::Ractor.new(pull) }
    ensure
      pull&.close
    end
  end


  # ── Without Async {} (automatic IO thread) ─────────────────

  it "raw pipeline without Async" do
    OMQ::Transport::Inproc.reset!
    pull = OMQ::PULL.bind("ruby://r-noasync-raw")
    push = OMQ::PUSH.bind("ruby://r-noasync-raw-out")

    worker = OMQ::Ractor.new(pull, push, serialize: false) do |omq|
      p_in, p_out = omq.sockets
      3.times { p_out << [p_in.receive.first.upcase] }
    end

    sender   = OMQ::PUSH.connect("ruby://r-noasync-raw")
    receiver = OMQ::PULL.connect("ruby://r-noasync-raw-out")
    sender.peer_connected.wait
    receiver.peer_connected.wait

    3.times { |i| sender << "msg-#{i}" }
    results = 3.times.map { receiver.receive.first }

    worker.join
    assert_equal %w[MSG-0 MSG-1 MSG-2], results
  ensure
    [sender, receiver, pull, push].compact.each(&:close)
  end


  it "serialized pipeline without Async" do
    OMQ::Transport::Inproc.reset!
    pull = OMQ::PULL.bind("ruby://r-noasync-ser")
    push = OMQ::PUSH.bind("ruby://r-noasync-ser-out")

    worker = OMQ::Ractor.new(pull, push) do |omq|
      p_in, p_out = omq.sockets
      3.times { p_out << { result: p_in.receive.upcase } }
    end

    sender   = OMQ::PUSH.connect("ruby://r-noasync-ser")
    receiver = OMQ::PULL.connect("ruby://r-noasync-ser-out")
    sender.peer_connected.wait
    receiver.peer_connected.wait

    3.times { |i| sender << "hello-#{i}" }
    results = 3.times.map { receiver.receive }

    worker.join
    assert_equal [[{ result: "HELLO-0" }], [{ result: "HELLO-1" }], [{ result: "HELLO-2" }]], results
  ensure
    [sender, receiver, pull, push].compact.each(&:close)
  end


  it "worker #value without Async" do
    OMQ::Transport::Inproc.reset!
    pull = OMQ::PULL.bind("ruby://r-noasync-value")

    worker = OMQ::Ractor.new(pull, serialize: false) do |omq|
      p = omq.sockets.first
      3.times.map { p.receive.first }
    end

    push = OMQ::PUSH.connect("ruby://r-noasync-value")
    push.peer_connected.wait

    3.times { |i| push << "v-#{i}" }

    result = worker.value
    assert_equal %w[v-0 v-1 v-2], result
  ensure
    [push, pull].compact.each(&:close)
  end
end
