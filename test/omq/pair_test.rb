# frozen_string_literal: true

require_relative "../test_helper"

class OMQPairTest < Minitest::Spec
  describe OMQ::PAIR do
    before do
      OMQ::Transport::Inproc.reset!
    end

    it "sends and receives a message over inproc" do
      Async do
        server = OMQ::PAIR.bind("ruby://pair-test-1")
        client = OMQ::PAIR.connect("ruby://pair-test-1")

        client.send("hello")
        msg = server.receive
        assert_equal ["hello"], msg

        server.send("world")
        msg = client.receive
        assert_equal ["world"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "handles multi-frame messages" do
      Async do
        server = OMQ::PAIR.bind("ruby://pair-test-2")
        client = OMQ::PAIR.connect("ruby://pair-test-2")

        client.send(["part1", "part2", "part3"])
        msg = server.receive
        assert_equal ["part1", "part2", "part3"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "supports << for chaining" do
      Async do
        server = OMQ::PAIR.bind("ruby://pair-test-3")
        client = OMQ::PAIR.connect("ruby://pair-test-3")

        result = client << "test"
        assert_same client, result

        msg = server.receive
        assert_equal ["test"], msg
      ensure
        client&.close
        server&.close
      end
    end

    it "returns parsed URI from #bind" do
      Async do
        server = OMQ::PAIR.new
        uri = server.bind("ruby://pair-test-5")
        assert_equal "ruby://pair-test-5", uri.to_s
      ensure
        server&.close
      end
    end


    it "delivers inproc messages sent before the peer bound" do
      Async do
        client = OMQ::PAIR.new.tap { it.linger = 0 }
        client.reconnect_interval = RECONNECT_INTERVAL
        client.connect("inproc://pair-early-send")
        client << "early"

        server = OMQ::PAIR.bind("inproc://pair-early-send")
        assert_equal ["early"], server.receive

        client << "later"
        assert_equal ["later"], server.receive
      ensure
        client&.close
        server&.close
      end
    end


    it "refuses a second inproc peer, which takes over after the first leaves" do
      Async do |task|
        server = OMQ::PAIR.new.tap { it.linger = 0 }
        server.bind("inproc://pair-second-peer")
        first = OMQ::PAIR.new.tap { it.linger = 0 }
        first.connect("inproc://pair-second-peer")

        second = OMQ::PAIR.new.tap { it.linger = 0 }
        second.reconnect_interval = RECONNECT_INTERVAL
        second.connect("inproc://pair-second-peer")
        task.yield until second.connection_count == 0
        assert_equal 1, server.connection_count

        first.close
        second.peer_connected.wait
        second << "from second"
        assert_equal ["from second"], server.receive
      ensure
        second&.close
        server&.close
      end
    end


    it "redials a refused second endpoint once the first peer leaves" do
      Async do
        a = OMQ::PAIR.new.tap { it.linger = 0 }
        b = OMQ::PAIR.new.tap { it.linger = 0 }
        a_port = a.bind("tcp://127.0.0.1:0").port
        b_port = b.bind("tcp://127.0.0.1:0").port

        client = OMQ::PAIR.new.tap { it.linger = 0 }
        client.reconnect_interval = RECONNECT_INTERVAL
        client.connect("tcp://127.0.0.1:#{a_port}")
        wait_connected(a)
        client.connect("tcp://127.0.0.1:#{b_port}")

        a.close
        wait_connected(b)
        client << "to b"
        assert_equal ["to b"], b.receive
      ensure
        client&.close
        b&.close
      end
    end
  end
end
