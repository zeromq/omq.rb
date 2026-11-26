# frozen_string_literal: true

require_relative "test_helper"
require "socket"
require "io/stream"

class ProtocolZMTPHandshakeLimitsTest < Minitest::Spec
  Codec = Protocol::ZMTP::Codec


  # Runs the server side of a NULL handshake against bytes written by
  # +peer+ and returns the error it raises.
  def server_handshake_error
    s1, s2 = UNIXSocket.pair
    sio    = IO::Stream::Buffered.wrap(s1)
    pio    = IO::Stream::Buffered.wrap(s2)
    server = Protocol::ZMTP::Connection.new(sio, socket_type: "ROUTER", as_server: true)
    error  = nil

    Async do |task|
      handshake = task.async do
        server.handshake!
      rescue Protocol::ZMTP::Error => e
        error = e
      end

      pio.read_exactly(64) # server greeting
      yield pio
      pio.flush
      handshake.wait
    end

    error
  ensure
    sio&.close
    pio&.close
  end


  describe "the handshake" do
    it "refuses a frame larger than HANDSHAKE_MAX_SIZE before reading it" do
      error = server_handshake_error do |peer|
        peer.write(Codec::Greeting.encode(mechanism: "NULL"))
        peer.write("\x06".b + [1 << 40].pack("Q>")) # long command frame, 1 TiB
      end

      assert_match(/exceeds/, error&.message)
    end


    it "refuses more than MAX_PROPERTIES properties" do
      extra = (1..Codec::Command::MAX_PROPERTIES).to_h { |i| ["X-#{i}", "v"] }

      error = server_handshake_error do |peer|
        peer.write(Codec::Greeting.encode(mechanism: "NULL"))
        peer.write(Codec::Command.ready(socket_type: "DEALER", identity: "", metadata: extra).to_frame.to_wire)
      end

      assert_match(/properties/, error&.message)
    end


    it "refuses a peer identity over 255 bytes" do
      error = server_handshake_error do |peer|
        peer.write(Codec::Greeting.encode(mechanism: "NULL"))
        peer.write(Codec::Command.ready(socket_type: "DEALER", identity: "x" * 256).to_frame.to_wire)
      end

      assert_match(/identity/, error&.message)
    end


    it "refuses an as-server flag other than 0 or 1" do
      greeting = Codec::Greeting.encode(mechanism: "NULL").dup
      greeting.setbyte(32, 2)

      assert_raises(Protocol::ZMTP::Error) { Codec::Greeting.decode(greeting) }
    end
  end
end
