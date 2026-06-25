# frozen_string_literal: true

require_relative "../test_helper"

describe "PUB/SUB" do
  before { OMQ::Transport::Inproc.reset! }

  it "delivers messages matching subscription" do
    Async do
      pub = OMQ::PUB.bind("ruby://pubsub-1")
      sub = OMQ::SUB.connect("ruby://pubsub-1")
      sub.subscribe("topic.")

      # Give subscription time to propagate
      Async::Task.current.yield

      pub.send("topic.hello")
      msg = sub.receive
      assert_equal ["topic.hello"], msg
    ensure
      sub&.close
      pub&.close
    end
  end

  it "SUB post-hoc connects to multiple PUBs and closes cleanly" do
    Sync do
      pub1 = OMQ::PUB.bind("ruby://pubsub-multi-1")
      pub2 = OMQ::PUB.bind("ruby://pubsub-multi-2")

      sub = OMQ::SUB.new
      sub.connect("ruby://pubsub-multi-1")
      sub.connect("ruby://pubsub-multi-2")
      sub.subscribe("")

      pub1.subscriber_joined.wait
      pub2.subscriber_joined.wait

      pub1.send("from-1")
      pub2.send("from-2")

      received = 2.times.map { sub.receive.first }
      assert_equal ["from-1", "from-2"].sort, received.sort
    ensure
      sub&.close
      pub1&.close
      pub2&.close
    end
  end

  it "fans out to multiple inproc subscribers" do
    Async do
      pub = OMQ::PUB.bind("ruby://pubsub-fanout-inproc")

      subs = 3.times.map { OMQ::SUB.connect("ruby://pubsub-fanout-inproc", subscribe: "") }

      pub.send("broadcast", "payload")

      subs.each do |sub|
        msg = sub.receive
        assert_equal ["broadcast", "payload"], msg
      end
    ensure
      subs&.each(&:close)
      pub&.close
    end
  end

  it "fans out to multiple IPC subscribers" do
    Async do
      pub = OMQ::PUB.bind("ipc://@omq-test-pubsub-fanout")

      subs = 3.times.map { OMQ::SUB.connect("ipc://@omq-test-pubsub-fanout", subscribe: "") }
      wait_connected(*subs)

      # Wait for subscriptions to propagate
      pub.subscriber_joined.wait
      Async::Task.current.yield

      pub.send("broadcast", "payload")

      subs.each do |sub|
        msg = sub.receive
        assert_equal ["broadcast", "payload"], msg
      end
    ensure
      subs&.each(&:close)
      pub&.close
    end
  end

  it "fans out to multiple TCP subscribers (pre-encoded wire path)" do
    Async do
      pub = OMQ::PUB.new
      port = pub.bind("tcp://127.0.0.1:0").port

      subs = 3.times.map { OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "") }
      wait_connected(*subs)

      pub.send("broadcast", "payload")

      subs.each do |sub|
        msg = sub.receive
        assert_equal ["broadcast", "payload"], msg
      end
    ensure
      subs&.each(&:close)
      pub&.close
    end
  end

  it "DropQueue :drop_newest drops incoming when full" do
    queue = OMQ::DropQueue.new(3, strategy: :drop_newest)
    3.times { |i| queue.enqueue("msg.#{i}") }

    # Queue is full — enqueue must not block, must silently drop
    elapsed = Async::Clock.measure do
      7.times { |i| queue.enqueue("overflow.#{i}") }
    end
    assert_operator elapsed, :<, 0.01

    # Only the original 3 messages should be in the queue
    drained = []
    loop do
      msg = queue.dequeue(timeout: 0)
      break unless msg
      drained << msg
    end
    assert_equal %w[msg.0 msg.1 msg.2], drained
  end

  it "DropQueue :drop_oldest evicts head when full" do
    queue = OMQ::DropQueue.new(3, strategy: :drop_oldest)
    3.times { |i| queue.enqueue("msg.#{i}") }

    # Overflow — should evict oldest each time
    queue.enqueue("new.0")
    queue.enqueue("new.1")

    drained = []
    loop do
      msg = queue.dequeue(timeout: 0)
      break unless msg
      drained << msg
    end
    assert_equal %w[msg.2 new.0 new.1], drained
  end

  it "does not block the publisher" do
    Async do
      pub = OMQ::PUB.new.tap { |s| s.linger = 0 }
      pub.send_hwm = 5
      pub.bind("ruby://pubsub-hwm")

      sub = OMQ::SUB.connect("ruby://pubsub-hwm", subscribe: "")

      # Flood with 200 messages — must complete without blocking
      elapsed = Async::Clock.measure do
        200.times { |i| pub.send("msg.#{i}") }
      end
      assert_operator elapsed, :<, 1.0
    ensure
      sub&.close
      pub&.close
    end
  end

  it "PUB defaults to on_mute: :drop_newest" do
    pub = OMQ::PUB.new.tap { |s| s.linger = 0 }
    assert_equal :drop_newest, pub.on_mute
  ensure
    pub&.close
  end

  it "SUB defaults to on_mute: :block" do
    sub = OMQ::SUB.new.tap { |s| s.linger = 0 }
    assert_equal :block, sub.on_mute
  ensure
    sub&.close
  end

  it "SUB accepts on_mute: :drop_oldest" do
    Async do
      pub = OMQ::PUB.new.tap { |s| s.linger = 0 }
      pub.send_hwm = 100
      pub.bind("ruby://pubsub-drop-oldest")

      sub = OMQ::SUB.new(nil, on_mute: :drop_oldest)
      sub.linger = 0
      sub.recv_hwm = 3
      sub.connect("ruby://pubsub-drop-oldest")
      sub.subscribe("")

      assert_equal :drop_oldest, sub.on_mute
    ensure
      sub&.close
      pub&.close
    end
  end

  it "set_unbounded works with PUB" do
    Async do
      pub = OMQ::PUB.new.tap { |s| s.linger = 0 }
      pub.set_unbounded
      pub.bind("ruby://pubsub-unbounded")

      sub = OMQ::SUB.new.tap { |s| s.linger = 0 }
      sub.set_unbounded
      sub.connect("ruby://pubsub-unbounded")
      sub.subscribe("")

      Async::Task.current.yield

      pub.send("hello")
      msg = sub.receive
      assert_equal ["hello"], msg
    ensure
      sub&.close
      pub&.close
    end
  end
end

describe "XPUB/XSUB" do
  before { OMQ::Transport::Inproc.reset! }

  it "XPUB receives subscription notifications" do
    Async do
      xpub = OMQ::XPUB.bind("ruby://xpub-sub-1")
      sub  = OMQ::SUB.new.tap { |s| s.linger = 0 }
      sub.connect("ruby://xpub-sub-1")
      sub.subscribe("weather.")

      msg = xpub.receive
      assert_equal 1, msg.size
      assert_equal "\x01weather.".b, msg.first
    ensure
      sub&.close
      xpub&.close
    end
  end

  it "XPUB delivers messages to matching subscribers" do
    Async do
      xpub = OMQ::XPUB.bind("ruby://xpub-sub-2")
      sub  = OMQ::SUB.connect("ruby://xpub-sub-2", subscribe: "news.")

      # Consume subscription notification
      xpub.receive

      xpub.send("news.headline")
      msg = sub.receive
      assert_equal ["news.headline"], msg
    ensure
      sub&.close
      xpub&.close
    end
  end

  it "XSUB sends subscriptions as data frames" do
    Async do
      pub  = OMQ::PUB.bind("ruby://pub-xsub-1")
      xsub = OMQ::XSUB.connect("ruby://pub-xsub-1")

      # Subscribe via XSUB data frame
      xsub.send("\x01stock.".b)

      # Wait for subscription to propagate
      Async::Task.current.yield

      pub.send("stock.AAPL")
      msg = xsub.receive
      assert_equal ["stock.AAPL"], msg
    ensure
      xsub&.close
      pub&.close
    end
  end

  it "XSUB subscribes via subscribe: kwarg" do
    Async do
      pub  = OMQ::PUB.bind("ruby://pub-xsub-2")
      xsub = OMQ::XSUB.connect("ruby://pub-xsub-2", subscribe: "fx.")

      Async::Task.current.yield

      pub.send("fx.EURUSD")
      msg = xsub.receive
      assert_equal ["fx.EURUSD"], msg
    ensure
      xsub&.close
      pub&.close
    end
  end

  it "XPUB receives unsubscription notifications" do
    Async do
      xpub = OMQ::XPUB.bind("ruby://xpub-unsub-1")
      sub  = OMQ::SUB.new.tap { |s| s.linger = 0 }
      sub.connect("ruby://xpub-unsub-1")
      sub.subscribe("topic.")

      # Consume subscribe notification
      msg = xpub.receive
      assert_equal "\x01topic.".b, msg.first

      sub.unsubscribe("topic.")

      # Should receive unsubscribe notification
      msg = xpub.receive
      assert_equal 1, msg.size
      assert_equal "\x00topic.".b, msg.first
    ensure
      sub&.close
      xpub&.close
    end
  end

  it "XPUB receives subscription notifications over TCP" do
    Async do
      xpub = OMQ::XPUB.new
      port = xpub.bind("tcp://127.0.0.1:0").port

      sub = OMQ::SUB.new.tap { |s| s.linger = 0 }
      sub.connect("tcp://127.0.0.1:#{port}")
      sub.subscribe("topic.")

      msg = xpub.receive
      assert_equal 1, msg.size
      assert_equal "\x01topic.".b, msg.first
    ensure
      sub&.close
      xpub&.close
    end
  end

  it "XPUB delivers filtered messages over TCP" do
    Async do
      xpub = OMQ::XPUB.new
      port = xpub.bind("tcp://127.0.0.1:0").port

      sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "news.")

      # Consume subscription notification
      xpub.receive

      xpub.send("news.headline")
      msg = sub.receive
      assert_equal ["news.headline"], msg
    ensure
      sub&.close
      xpub&.close
    end
  end
end
