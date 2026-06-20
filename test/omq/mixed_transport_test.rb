# frozen_string_literal: true

require_relative "../test_helper"

describe "mixed transports" do
  before { OMQ::Transport::Inproc.reset! }

  # Waits until a socket has the expected number of peer connections.
  def wait_connection_count(socket, count, timeout: 2)
    Async::Task.current.with_timeout(timeout) do
      sleep 0.001 until socket.connection_count >= count
    end
  end

  it "PUSH distributes across inproc and TCP peers" do
    Async do
      push = OMQ::PUSH.new
      push.bind("ruby://mixed-push-#{object_id}")
      tcp_port = push.bind("tcp://127.0.0.1:0").port

      pull_inproc = OMQ::PULL.connect("ruby://mixed-push-#{object_id}")
      push << "inproc-only"
      assert_equal ["inproc-only"], pull_inproc.receive

      pull_tcp = OMQ::PULL.connect("tcp://127.0.0.1:#{tcp_port}")
      wait_connection_count(push, 2)

      pull_inproc.read_timeout = 0.02
      pull_tcp.read_timeout    = 0.02
      4.times { |i| push << "mixed-#{i}" }
      received = 0
      loop do
        pull_inproc.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      loop do
        pull_tcp.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      assert_equal 4, received
    ensure
      push&.close
      pull_inproc&.close
      pull_tcp&.close
    end
  end


  it "reverts to direct pipe after TCP peer disconnects" do
    Async do
      push = OMQ::PUSH.new
      push.bind("ruby://mixed-revert-#{object_id}")
      tcp_port = push.bind("tcp://127.0.0.1:0").port

      pull_inproc = OMQ::PULL.connect("ruby://mixed-revert-#{object_id}")
      pull_tcp    = OMQ::PULL.connect("tcp://127.0.0.1:#{tcp_port}")
      wait_connection_count(push, 2)

      # Both connected — round-robin
      pull_inproc.read_timeout = 0.02
      pull_tcp.read_timeout    = 0.02
      2.times { |i| push << "both-#{i}" }
      received = 0
      loop do
        pull_inproc.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      loop do
        pull_tcp.receive
        received += 1
      rescue IO::TimeoutError
        break
      end
      assert_equal 2, received

      # TCP peer disconnects — back to single inproc peer
      pull_tcp.close
      pull_tcp = nil
      sleep 0.01 until push.connection_count == 1

      push << "inproc-again"
      pull_inproc.read_timeout = nil
      assert_equal ["inproc-again"], pull_inproc.receive
    ensure
      push&.close
      pull_inproc&.close
      pull_tcp&.close
    end
  end


  it "REQ/REP over TCP after inproc warmup" do
    Async do
      rep = OMQ::REP.new
      port = rep.bind("tcp://127.0.0.1:0").port
      req = OMQ::REQ.connect("tcp://127.0.0.1:#{port}")
      wait_connected(req)

      3.times do |i|
        req << "ping-#{i}"
        msg = rep.receive
        assert_equal ["ping-#{i}"], msg
        rep << "pong-#{i}"
        reply = req.receive
        assert_equal ["pong-#{i}"], reply
      end
    ensure
      req&.close
      rep&.close
    end
  end
end
