# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS 1 PUSH/PULL" do
  before { OMQ::Transport::Inproc.reset! }

  describe "over inproc" do
    it "sends and receives with ACK" do
      Sync do
        pull = OMQ::PULL.new
        pull.qos = OMQ::QoS.at_least_once
        pull.bind("ruby://qos1-pp-1")

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger = 1
        push.connect("ruby://qos1-pp-1")
        wait_connected(push)

        push.send("hello")
        assert_equal ["hello"], pull.receive

        push.send(["multi", "part"])
        assert_equal ["multi", "part"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "delivers many messages reliably" do
      Sync do
        pull = OMQ::PULL.new
        pull.qos = OMQ::QoS.at_least_once
        pull.bind("ruby://qos1-pp-many")

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger = 1
        push.connect("ruby://qos1-pp-many")
        wait_connected(push)

        n = 100
        n.times { |i| push.send("msg-#{i}") }

        received = Array.new(n) { pull.receive.first }

        assert_equal n, received.size
        assert_equal "msg-0", received.first
        assert_equal "msg-#{n - 1}", received.last
      ensure
        push&.close
        pull&.close
      end
    end
  end


  describe "over TCP" do
    it "sends and receives with ACK" do
      Sync do
        pull = OMQ::PULL.new
        pull.qos = OMQ::QoS.at_least_once
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger             = 1
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{port}")
        wait_connected(push)

        push.send("hello-tcp")
        assert_equal ["hello-tcp"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "replays multiple in-flight messages on connection loss" do
      Sync do
        pull1 = OMQ::PULL.new
        pull1.qos = OMQ::QoS.at_least_once
        pull1.recv_hwm = 1
        pull1.linger   = 0
        port1 = pull1.bind("tcp://127.0.0.1:0").port

        pull2 = OMQ::PULL.new
        pull2.qos = OMQ::QoS.at_least_once
        pull2.linger = 1
        port2 = pull2.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger             = 2
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{port1}")
        push.connect("tcp://127.0.0.1:#{port2}")
        wait_connected(push)

        n = 10
        n.times { |i| push.send("msg-#{i}") }
        sleep 0.1

        pull1.close
        pull1 = nil

        # QoS 1 ACKs after enqueue into the receiver's recv_queue. With
        # recv_hwm=1, at most 1 message is enqueued and ACK'd before the
        # recv pump blocks, so at most 1 message can be "stored but never
        # consumed" when pull1 closes. The remaining n-1 stay un-ACK'd on
        # the sender and replay to pull2.
        received = []
        pull2.read_timeout = 1.0
        loop do
          received << pull2.receive.first
          break if received.size >= n - 1
        rescue IO::TimeoutError
          break
        end

        sent = Array.new(n) { |i| "msg-#{i}" }
        unexpected = received - sent
        missing = sent - received

        assert unexpected.empty?,
               "unexpected on pull2: #{unexpected.inspect}, got: #{received.inspect}"
        assert missing.size <= 1,
               "missing too many from pull2: #{missing.inspect}, got: #{received.inspect}"
      ensure
        push&.close
        pull1&.close
        pull2&.close
      end
    end


    it "replays pending messages when the peer process is SIGKILL'd" do
      r, w = IO.pipe
      child = fork do
        r.close
        Sync do
          pull = OMQ::PULL.new
          pull.qos = OMQ::QoS.at_least_once
          pull.recv_hwm = 1
          w.puts pull.bind("tcp://127.0.0.1:0").port
          w.close
          sleep 30
        end
      end
      w.close
      child_port = r.gets.to_i
      r.close

      Sync do
        backup = OMQ::PULL.new
        backup.qos = OMQ::QoS.at_least_once
        backup.linger = 1
        backup_port = backup.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger             = 2
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{child_port}")
        push.connect("tcp://127.0.0.1:#{backup_port}")
        wait_connected(push)

        n = 10
        n.times { |i| push.send("k#{i}") }
        sleep 0.1

        Process.kill(:KILL, child)
        Process.wait(child)
        child = nil

        # See comment on the graceful-close variant above — at most
        # recv_hwm (=1) messages may be stored-but-never-consumed on
        # the killed peer and thus not replay.
        received = []
        backup.read_timeout = 1.0
        loop do
          received << backup.receive.first
          break if received.size >= n - 1
        rescue IO::TimeoutError
          break
        end
        sent = Array.new(n) { |i| "k#{i}" }
        unexpected = received - sent
        missing = sent - received

        assert unexpected.empty?,
               "unexpected: #{unexpected.inspect}, got: #{received.inspect}"
        assert missing.size <= 1,
               "missing too many: #{missing.inspect}, got: #{received.inspect}"
      ensure
        push&.close
        backup&.close
      end
    ensure
      if child
        Process.kill(:KILL, child) rescue nil
        Process.wait(child) rescue nil
      end
    end


    it "retries to next PULL on disconnect" do
      Sync do
        pull1 = OMQ::PULL.new
        pull1.qos = OMQ::QoS.at_least_once
        pull1.linger = 0
        port1 = pull1.bind("tcp://127.0.0.1:0").port

        pull2 = OMQ::PULL.new
        pull2.qos = OMQ::QoS.at_least_once
        pull2.linger = 0
        port2 = pull2.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new
        push.qos = OMQ::QoS.at_least_once
        push.linger             = 1
        push.reconnect_interval = RECONNECT_INTERVAL
        push.connect("tcp://127.0.0.1:#{port1}")
        push.connect("tcp://127.0.0.1:#{port2}")
        wait_connected(push)

        pull1.close
        pull1 = nil
        sleep 0.1

        push.send("after")
        assert_equal ["after"], pull2.receive
      ensure
        push&.close
        pull1&.close
        pull2&.close
      end
    end
  end
end


describe "QoS 0 PUSH/PULL unchanged" do
  before { OMQ::Transport::Inproc.reset! }

  it "works without ACK at QoS 0 (default)" do
    Sync do
      pull = OMQ::PULL.bind("ruby://qos0-pp")
      push = OMQ::PUSH.connect("ruby://qos0-pp")

      push.send("no-ack")
      assert_equal ["no-ack"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end
end
