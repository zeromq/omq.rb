# frozen_string_literal: true

require_relative "../test_helper"
require "socket"
require "io/stream"

describe Protocol::ZMTP::Mechanism::Null do
  it "is not encrypted" do
    mech = Protocol::ZMTP::Mechanism::Null.new
    refute mech.encrypted?
  end


  it "always sends as-server=0 in the greeting (RFC 23)" do
    Async do
      s1, s2 = UNIXSocket.pair
      sio = IO::Stream::Buffered.wrap(s1)
      cio = IO::Stream::Buffered.wrap(s2)

      server = Protocol::ZMTP::Connection.new(sio, socket_type: "REP", as_server: true)
      client = Protocol::ZMTP::Connection.new(cio, socket_type: "REQ", as_server: false)

      Barrier do |bar|
        bar.async { server.handshake! }
        bar.async do
          raw = cio.read_exactly(64)
          assert_equal 0x00, raw.getbyte(32), "server NULL greeting must have as-server=0"

          cio.write(Protocol::ZMTP::Codec::Greeting.encode(mechanism: "NULL", as_server: false))
          cio.flush
          ready = Protocol::ZMTP::Codec::Command.ready(socket_type: "REQ", identity: "")
          cio.write(ready.to_frame.to_wire)
          cio.flush
          Protocol::ZMTP::Codec::Frame.read_from(cio)
        end
      end
    ensure
      sio&.close
      cio&.close
    end
  end
end
