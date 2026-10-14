# frozen_string_literal: true

require_relative "../test_helper"
require "socket"
require "io/stream"

# Verifies OMQ PUB/SUB is backwards-compatible with ZMTP 3.0 peers
# (libzmq, JeroMQ, pyzmq), which send subscriptions as message-form
# `\x01<prefix>` / `\x00<prefix>` data frames rather than ZMTP 3.1
# SUBSCRIBE/CANCEL command frames.
#
# The version the peer advertises in its 64-byte greeting determines
# which wire form OMQ uses when *sending* subscriptions to that peer.
#
class OMQZMTP30CompatTest < Minitest::Spec
  describe "ZMTP 3.0 PUB/SUB compatibility" do
    Frame     = Protocol::ZMTP::Codec::Frame
    Greeting  = Protocol::ZMTP::Codec::Greeting
    Command   = Protocol::ZMTP::Codec::Command

    # Builds a NULL greeting with a configurable minor version.
    def make_greeting(minor:, as_server:)
      greeting = Greeting.encode(mechanism: "NULL", as_server: as_server).b
      greeting.setbyte(11, minor)
      greeting
    end


    # Acts as a fake ZMTP peer behind a one-shot TCP listener.
    # Runs the NULL handshake by hand with the given minor version,
    # then yields the io-stream buffer to the block.
    #
    def serve_fake_peer(minor:, socket_type:, identity: "")
      listener = TCPServer.new("127.0.0.1", 0)
      port     = listener.addr[1]

      task = Async do
        client = listener.accept
        listener.close
        io = IO::Stream::Buffered.wrap(client)

        io.write(make_greeting(minor: minor, as_server: true))
        io.flush
        io.read_exactly(Greeting::SIZE) # peer greeting

        ready_frame = Frame.read_from(io)
        raise "expected command" unless ready_frame.command?
        Command.from_body(ready_frame.body) # READY

        io.write(Command.ready(socket_type: socket_type, identity: identity).to_frame.to_wire)
        io.flush

        yield io
      ensure
        io&.close
      end

      [port, task]
    end


    it "OMQ SUB sends message-form subscription to a ZMTP 3.0 PUB peer" do
      Async do
        port, task = serve_fake_peer(minor: 0, socket_type: "PUB") do |io|
          frame = Frame.read_from(io)
          refute frame.command?, "expected data frame, got command (ZMTP 3.1 form)"
          assert_equal "\x01topic.".b, frame.body
        end

        sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "topic.")
        task.wait
      ensure
        sub&.close
      end
    end


    it "OMQ SUB sends command-form SUBSCRIBE to a ZMTP 3.1 PUB peer" do
      Async do
        port, task = serve_fake_peer(minor: 1, socket_type: "PUB") do |io|
          frame = Frame.read_from(io)
          assert frame.command?, "expected command frame (ZMTP 3.1 SUBSCRIBE)"
          cmd = Command.from_body(frame.body)
          assert_equal "SUBSCRIBE", cmd.name
          assert_equal "topic.", cmd.data
        end

        sub = OMQ::SUB.connect("tcp://127.0.0.1:#{port}", subscribe: "topic.")
        task.wait
      ensure
        sub&.close
      end
    end


    it "OMQ XSUB sends message-form to a ZMTP 3.0 PUB peer" do
      Async do
        port, task = serve_fake_peer(minor: 0, socket_type: "PUB") do |io|
          frame = Frame.read_from(io)
          refute frame.command?
          assert_equal "\x01xs.".b, frame.body
        end

        xsub = OMQ::XSUB.connect("tcp://127.0.0.1:#{port}")
        wait_connected(xsub)
        xsub.send("\x01xs.".b)
        task.wait
      ensure
        xsub&.close
      end
    end


    it "OMQ XSUB sends command-form to a ZMTP 3.1 PUB peer" do
      Async do
        port, task = serve_fake_peer(minor: 1, socket_type: "PUB") do |io|
          frame = Frame.read_from(io)
          assert frame.command?
          cmd = Command.from_body(frame.body)
          assert_equal "SUBSCRIBE", cmd.name
          assert_equal "xs.", cmd.data
        end

        xsub = OMQ::XSUB.connect("tcp://127.0.0.1:#{port}")
        wait_connected(xsub)
        xsub.send("\x01xs.".b)
        task.wait
      ensure
        xsub&.close
      end
    end


    it "OMQ PUB accepts message-form SUBSCRIBE from a ZMTP 3.0 SUB peer" do
      Async do
        pub  = OMQ::PUB.new
        port = pub.bind("tcp://127.0.0.1:0").port

        received = nil
        task = Async do
          raw = TCPSocket.new("127.0.0.1", port)
          io  = IO::Stream::Buffered.wrap(raw)

          io.write(make_greeting(minor: 0, as_server: false))
          io.flush
          io.read_exactly(Greeting::SIZE)

          io.write(Command.ready(socket_type: "SUB", identity: "").to_frame.to_wire)
          io.flush
          Frame.read_from(io) # peer READY

          # Message-form subscribe (ZMTP 3.0).
          io.write(Frame.new("\x01news.".b).to_wire)
          io.flush

          frame = Frame.read_from(io)
          received = frame.body
        ensure
          io&.close
        end

        pub.subscriber_joined.wait
        pub.send("news.flash")
        task.wait

        assert_equal "news.flash".b, received
      ensure
        pub&.close
      end
    end

  end
end
