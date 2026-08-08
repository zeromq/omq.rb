# frozen_string_literal: true

require_relative "../test_helper"

describe "Stress tests" do
  before { OMQ::Transport::Inproc.reset! }

  it "handles 10k messages through PUSH/PULL inproc" do
    n = 10_000
    Async do
      pull = OMQ::PULL.bind("ruby://stress-pushpull")
      push = OMQ::PUSH.connect("ruby://stress-pushpull")

      sender = Async do
        n.times { |i| push.send("msg-#{i}") }
      end

      received = 0
      n.times do
        pull.receive
        received += 1
      end

      sender.wait
      assert_equal n, received
    ensure
      push&.close
      pull&.close
    end
  end

  it "handles 1k messages through REQ/REP over TCP" do
    n = 1_000
    Async do |task|
      rep = OMQ::REP.new
      port = rep.bind("tcp://127.0.0.1:0").port
      req = OMQ::REQ.connect("tcp://127.0.0.1:#{port}")

      responder = task.async do
        n.times do
          msg = rep.receive
          rep << msg
        end
      end

      n.times do |i|
        req << "req-#{i}"
        reply = req.receive
        assert_equal ["req-#{i}"], reply
      end

      responder.wait
    ensure
      req&.close
      rep&.close
    end
  end

  it "handles multiple concurrent DEALER connections to ROUTER" do
    n_dealers = 5
    n_msgs    = 100

    Async do
      router = OMQ::ROUTER.bind("ruby://stress-router")

      dealers = n_dealers.times.map do |id|
        d = OMQ::DEALER.new
        d.identity = "dealer-#{id}"
        d.connect("ruby://stress-router")
        d
      end

      # Each dealer sends n_msgs messages
      senders = dealers.map do |d|
        Async do
          n_msgs.times { |i| d.send("msg-#{i}") }
        end
      end

      # Router receives all messages
      total    = n_dealers * n_msgs
      received = Hash.new(0)
      total.times do
        msg = router.receive
        identity = msg[0]
        received[identity] += 1
      end

      senders.each(&:wait)

      assert_equal n_dealers, received.size
      received.each do |identity, count|
        assert_equal n_msgs, count, "#{identity} sent #{count}/#{n_msgs}"
      end
    ensure
      dealers&.each(&:close)
      router&.close
    end
  end

  it "handles PUB/SUB fan-out to multiple subscribers" do
    n_subs = 5
    n_msgs = 50

    Async do
      pub = OMQ::PUB.bind("ruby://stress-pubsub")

      subs = n_subs.times.map do
        OMQ::SUB.connect("ruby://stress-pubsub", subscribe: "")
      end

      # Wait until every subscriber has received from this PUB.
      # #subscriber_joined only means the first subscription reached PUB.
      ready = Array.new(n_subs, false)
      sync_barrier = Async::Barrier.new

      subs.each_with_index do |sub, i|
        sub.read_timeout = 0.02
        sync_barrier.async do
          until ready[i]
            begin
              ready[i] = sub.receive == ["__omq_sync__"]
            rescue IO::TimeoutError
            end
          end
        end
      end

      begin
        Async::Task.current.with_timeout(0.5) do
          until ready.all?
            pub.send("__omq_sync__")
            sleep 0.001
          end
          sync_barrier.wait
        end
      ensure
        sync_barrier.stop
      end

      # Start receivers first, then publish the real batch.
      barrier = Async::Barrier.new

      counts = Array.new(n_subs)
      subs.each_with_index do |sub, i|
        sub.read_timeout = nil
        barrier.async do
          count = 0
          begin
            Async::Task.current.with_timeout(0.5) do
              until count == n_msgs
                msg = sub.receive
                count += 1 if msg.first.start_with?("msg-")
              end
            end
          rescue Async::TimeoutError
          ensure
            counts[i] = count
          end
        end
      end

      sleep 0
      n_msgs.times { |i| pub.send("msg-#{i}") }
      barrier.wait

      counts.each_with_index do |count, i|
        assert_equal n_msgs, count, "subscriber #{i} received #{count}/#{n_msgs}"
      end
    ensure
      subs&.each(&:close)
      pub&.close
    end
  end

  it "handles large messages (1MB)" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("tcp://127.0.0.1:0").port
      push = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")

      big = "x" * 1_000_000
      push.send(big)
      msg = pull.receive
      assert_equal 1_000_000, msg.first.bytesize
    ensure
      push&.close
      pull&.close
    end
  end
end
