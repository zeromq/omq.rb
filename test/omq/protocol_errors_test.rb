# frozen_string_literal: true

require_relative "../test_helper"
require "io/stream"

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
