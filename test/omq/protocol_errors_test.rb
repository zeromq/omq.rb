# frozen_string_literal: true

require_relative "../test_helper"
require "io/stream"
require "omq/client_server"
require "omq/scatter_gather"
require "omq/channel"
require "omq/peer"

class OMQProtocolErrorsTest < Minitest::Spec
  Z = Protocol::ZMTP
  F = Z::Codec::Frame
  C = Z::Codec::Command
  G = Z::Codec::Greeting

  it "short ping does not poison other peers" do
    Async do |task|
      task.with_timeout(2) do
        rep = OMQ::REP.new(linger: 0)
        port = rep.bind("tcp://127.0.0.1:0").port
        req = OMQ::REQ.new(linger: 0)
        req.connect("tcp://127.0.0.1:#{port}")
        req.peer_connected.wait

        raw = IO::Stream::Buffered.wrap(TCPSocket.new("127.0.0.1", port))
        raw.write(G.encode + C.ready(socket_type: "DEALER").to_frame.to_wire)
        raw.flush
        G.read_from(raw)
        F.read_from(raw)
        raw.write(C.new("PING", "\x00".b).to_frame.to_wire)
        raw.flush
        sleep 0.02

        req << "healthy-request"
        assert_equal ["healthy-request"], rep.receive
      ensure
        raw&.close
        req&.close
        rep&.close
      end
    end.wait
  end

  {
    OMQ::GATHER  => "SCATTER",
    OMQ::CLIENT  => "SERVER",
    OMQ::SERVER  => "CLIENT",
    OMQ::CHANNEL => "CHANNEL",
    OMQ::PEER    => "PEER",
  }.each do |klass, peer_type|
    it "#{klass.name.split("::").last} drops multipart input from a peer" do
      Async do
        sock = klass.new(linger: 0)
        port = sock.bind("tcp://127.0.0.1:0").port
        raw  = raw_zmtp_peer(port, peer_type)

        raw.send_message(["part 1".b, "part 2".b])
        raw.send_message(["single".b])

        msg = sock.receive
        assert_equal "single", msg.last
        assert_includes [1, 2], msg.size # SERVER/PEER prepend a routing id
      ensure
        raw&.close
        sock&.close
      end
    end
  end


  it "drops a peer that sends more parts than max_message_parts" do
    Async do
      pull = OMQ::PULL.new
      pull.linger            = 0
      pull.max_message_parts = 8
      port = pull.bind("tcp://127.0.0.1:0").port

      raw = raw_zmtp_peer(port, "PUSH")
      raw.send_message(Array.new(8) { "".b })
      assert_equal 8, pull.receive.size

      raw.send_message(Array.new(9) { "".b })
      pull.all_peers_gone.wait
      pass
    ensure
      raw&.close
      pull&.close
    end
  end


  it "drops a subscriber that holds more than max_peer_subscriptions" do
    Async do
      pub = OMQ::PUB.new
      pub.linger                 = 0
      pub.max_peer_subscriptions = 4
      port = pub.bind("tcp://127.0.0.1:0").port

      raw = raw_zmtp_peer(port, "SUB")
      %w[a b c d d].each { |prefix| raw.send_command(Z::Codec::Command.subscribe(prefix)) }
      pub.subscriber_joined.wait
      assert_equal 1, pub.connection_count # 4 distinct prefixes are fine

      raw.send_command(Z::Codec::Command.subscribe("e"))
      pub.all_peers_gone.wait
      pass
    ensure
      raw&.close
      pub&.close
    end
  end


  it "ROUTER replaces a peer identity that starts with a zero byte" do
    Async do
      router = OMQ::ROUTER.new(linger: 0)
      port   = router.bind("tcp://127.0.0.1:0").port
      raw    = raw_zmtp_peer(port, "DEALER", identity: "\0taken".b)

      raw.send_message(["hello".b])
      identity, body = router.receive

      assert_equal "hello", body
      refute_equal "\0taken".b, identity
      assert_equal 0, identity.getbyte(0)
      assert_equal 5, identity.bytesize
    ensure
      raw&.close
      router&.close
    end
  end


  it "fatal error stops reconnect" do
    Async do |task|
      task.with_timeout(2) do
        listener = TCPServer.new("127.0.0.1", 0)
        accepts = 0
        server = task.async do
          loop do
            io = IO::Stream::Buffered.wrap(listener.accept)
            accepts += 1
            io.write(G.encode)
            io.flush
            G.read_from(io)
            F.read_from(io)
            io.write(C.new("ERROR", "\x06denied".b).to_frame.to_wire)
            io.flush
            io.close
          end
        end
        req = OMQ::REQ.new(linger: 0)
        req.reconnect_interval = 0.01
        req.connect("tcp://127.0.0.1:#{listener.addr[1]}")
        sleep 0.12
        assert_equal 1, accepts
      ensure
        req&.close
        server&.stop
        listener&.close
      end
    end.wait
  end
end
