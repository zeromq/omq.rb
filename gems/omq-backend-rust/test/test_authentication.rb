# frozen_string_literal: true

require_relative "test_helper"
require "protocol/zmtp/mechanism/plain"
require "protocol/zmtp/mechanism/curve"
require "nuckle"

class OMQBackendRustAuthenticationTest < Minitest::Spec
  Curve = Protocol::ZMTP::Mechanism::Curve

  def java_authenticator_available?
    return true unless RUBY_ENGINE == "jruby"

    ::Java::JavaLang::Class.forName("io.omq.PeerInfo", false, ::Java::JavaLang::ClassLoader.getSystemClassLoader)
    true
  rescue ::Java::JavaLang::ClassNotFoundException
    false
  end

  describe "security configuration" do
    it "rejects PLAIN and custom mechanisms before creating a native socket" do
      [Protocol::ZMTP::Mechanism::Plain.new, Object.new].each do |mechanism|
        socket = OMQ::PULL.new(backend: :rust)
        socket.mechanism = mechanism
        error = assert_raises(ArgumentError) { socket.bind("tcp://127.0.0.1:0") }
        assert_includes error.message, "does not support security mechanism"
      ensure
        socket&.close
      end
    end

    if RUBY_ENGINE == "jruby"
      it "rejects authenticators when JNI cannot load the callback peer class" do
        skip "OMQ.java is on the JVM classpath" if java_authenticator_available?

        key    = Nuckle::PrivateKey.generate
        socket = OMQ::PULL.new(backend: :rust)
        socket.mechanism = Curve.server(
          public_key: key.public_key.to_s, secret_key: key.to_s, crypto: Nuckle,
          authenticator: ->(_) { false },
        )
        error = assert_raises(ArgumentError) { socket.bind("tcp://127.0.0.1:0") }
        assert_includes error.message, "JVM CLASSPATH"
      ensure
        socket&.close
      end
    end
  end

  [:ruby, :rust].each do |client_backend|
    describe "CURVE authentication with a #{client_backend} client" do
      [true, false].each do |accept|
        it "#{accept ? 'accepts' : 'rejects'} according to the configured authenticator" do
          skip_without_ruby_backend if client_backend == :ruby
          skip "JNI callbacks require OMQ.java on the JVM classpath" unless java_authenticator_available?

          server_key = Nuckle::PrivateKey.generate
          client_key = Nuckle::PrivateKey.generate
          peers      = Thread::Queue.new

          run_backend do |task|
            task.with_timeout(3) do
              pull = OMQ::PULL.new(backend: :rust, recv_timeout: 0.1)
              pull.mechanism = Curve.server(
                public_key: server_key.public_key.to_s, secret_key: server_key.to_s, crypto: Nuckle,
                authenticator: ->(peer) { peers << peer; accept },
              )
              port = pull.bind("tcp://127.0.0.1:0").port
              push = OMQ::PUSH.new(backend: client_backend, linger: 0)
              push.mechanism = Curve.client(
                public_key: client_key.public_key.to_s, secret_key: client_key.to_s,
                server_key: server_key.public_key.to_s, crypto: Nuckle,
              )
              push.connect("tcp://127.0.0.1:#{port}")

              peer = peers.pop
              assert_instance_of Protocol::ZMTP::PeerInfo, peer
              assert_instance_of Nuckle::PublicKey, peer.public_key
              assert_equal client_key.public_key.to_s, peer.public_key.to_s
              assert_equal "", peer.identity

              if accept
                push.peer_connected.wait
                push << "authorized"
                assert_equal ["authorized"], pull.receive
              else
                push << "unauthorized"
                assert_raises(IO::TimeoutError) { pull.receive }
                assert_empty pull.engine.connections
              end
            ensure
              push&.close
              pull&.close
            end
          end.wait
        end
      end
    end
  end
end
