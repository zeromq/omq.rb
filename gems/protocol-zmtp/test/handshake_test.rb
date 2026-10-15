# frozen_string_literal: true

require_relative "test_helper"
require "protocol/zmtp/mechanism/curve"
require "protocol/zmtp/mechanism/plain"
require "nuckle"

class ProtocolZMTPHandshakeTest < Minitest::Spec
  ZMTP    = Protocol::ZMTP
  Command = ZMTP::Codec::Command

  def with_lowercase_metadata
    encoder = Command.method(:encode_properties)
    Command.define_singleton_method(:encode_properties) do |props|
      encoder.call(props.transform_keys(&:downcase))
    end
    yield
  ensure
    Command.define_singleton_method(:encode_properties, encoder)
  end

  def mechanisms(name)
    case name
    when :null
      [ZMTP::Mechanism::Null.new, ZMTP::Mechanism::Null.new]
    when :plain
      [ZMTP::Mechanism::Plain.new, ZMTP::Mechanism::Plain.new(username: "user", password: "pass")]
    when :curve
      secret = Nuckle::PrivateKey.generate
      [
        ZMTP::Mechanism::Curve.server(public_key: secret.public_key.to_s, secret_key: secret.to_s, crypto: Nuckle),
        ZMTP::Mechanism::Curve.client(server_key: secret.public_key.to_s, crypto: Nuckle),
      ]
    end
  end

  [:null, :plain, :curve].each do |name|
    describe "#{name} handshake" do
      it "accepts lowercase metadata from both peers" do
        with_lowercase_metadata do
          Async do |task|
            task.with_timeout(1) do
              s1, s2      = UNIXSocket.pair
              sio         = IO::Stream::Buffered.wrap(s1)
              cio         = IO::Stream::Buffered.wrap(s2)
              sm, cm      = mechanisms(name)
              server      = ZMTP::Connection.new(sio, socket_type: "ROUTER", identity: "server", as_server: true, mechanism: sm)
              client      = ZMTP::Connection.new(cio, socket_type: "DEALER", identity: "client", mechanism: cm)
              server_task = task.async { server.handshake! }

              client.handshake!
              server_task.wait
              assert_equal "DEALER", server.peer_socket_type
              assert_equal "client", server.peer_identity
              assert_equal "ROUTER", client.peer_socket_type
              assert_equal "server", client.peer_identity
              assert_equal "client", server.peer_properties.fetch("identity")
            ensure
              sio&.close
              cio&.close
            end
          end.wait
        end
      end

      it "recognizes an ERROR before the first handshake response" do
        Async do |task|
          task.with_timeout(1) do
            s1, s2 = UNIXSocket.pair
            sio    = IO::Stream::Buffered.wrap(s1)
            cio    = IO::Stream::Buffered.wrap(s2)
            _, cm  = mechanisms(name)
            client = ZMTP::Connection.new(cio, socket_type: "DEALER", mechanism: cm)
            server_task = task.async do
              sio.write(ZMTP::Codec::Greeting.encode(mechanism: name.to_s.upcase, as_server: name != :null))
              sio.flush
              ZMTP::Codec::Greeting.read_from(sio)
              ZMTP::Codec::Frame.read_from(sio)
              sio.write(Command.error("denied").to_frame.to_wire)
              sio.flush
            end

            error = assert_raises(ZMTP::FatalHandshakeError) { client.handshake! }
            assert_includes error.message, "denied"
            server_task.wait
          ensure
            sio&.close
            cio&.close
          end
        end.wait
      end
    end
  end
end
