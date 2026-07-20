# frozen_string_literal: true

require_relative "../test_helper"

describe "WebSocket transport (ws://)" do
  it "advertises ws and wss schemes" do
    assert_equal OMQ::Transport::WebSocket, OMQ::Engine.transports["ws"]
    assert_equal OMQ::Transport::WebSocket, OMQ::Engine.transports["wss"]
  end


  it "advertises its Connection class for the engine handshake hook" do
    assert_equal OMQ::Transport::WebSocket::Connection, OMQ::Transport::WebSocket.connection_class
  end


  it "PUSH/PULL round-trips a multipart message over ws://" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("ws://127.0.0.1:0").port

      push = OMQ::PUSH.connect("ws://127.0.0.1:#{port}")

      push.send(["topic-a", "payload-1", "payload-2"])

      msg = pull.receive
      assert_equal ["topic-a", "payload-1", "payload-2"], msg
    ensure
      push&.close
      pull&.close
    end
  end


  it "REQ/REP round-trips an envelope over ws://" do
    Async do
      rep  = OMQ::REP.new
      port = rep.bind("ws://127.0.0.1:0").port

      req = OMQ::REQ.connect("ws://127.0.0.1:#{port}")

      req.send("question?")
      assert_equal ["question?"], rep.receive

      rep.send("answer.")
      assert_equal ["answer."], req.receive
    ensure
      req&.close
      rep&.close
    end
  end


  it "PUB/SUB filters by subscription prefix over ws://" do
    Async do |task|
      pub  = OMQ::PUB.new
      port = pub.bind("ws://127.0.0.1:0").port

      sub = OMQ::SUB.new(subscribe: "topic-a")
      sub.connect("ws://127.0.0.1:#{port}")

      # Wait for the subscription to propagate to the publisher.
      sleep 0.05

      pub.send(["topic-b", "ignored"])
      pub.send(["topic-a", "delivered"])

      msg = sub.receive
      assert_equal ["topic-a", "delivered"], msg
    ensure
      sub&.close
      pub&.close
    end
  end


  it "rejects requests on the wrong path with 404" do
    Async do
      pull = OMQ::PULL.new
      pull.ws_path = "/zeromq"
      port = pull.bind("ws://127.0.0.1:0").port

      # Direct HTTP probe: requesting /other-path should return 404.
      require "async/http/client"
      require "async/http/endpoint"

      client_endpoint = ::Async::HTTP::Endpoint.parse("http://127.0.0.1:#{port}/other-path")
      client          = ::Async::HTTP::Client.new(client_endpoint)
      response        = client.get("/other-path")

      assert_equal 404, response.status
      response.finish
      client.close
    ensure
      pull&.close
    end
  end


  it "PAIR over wss:// with a self-signed TLS context" do
    Async do
      ctx = build_self_signed_tls_context

      server = OMQ::PAIR.new
      server.tls_context = ctx
      port = server.bind("wss://127.0.0.1:0").port

      client_ctx = OpenSSL::SSL::SSLContext.new
      client_ctx.verify_mode = OpenSSL::SSL::VERIFY_NONE
      client_ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION

      client = OMQ::PAIR.new
      client.tls_context = client_ctx
      client.connect("wss://127.0.0.1:#{port}")

      client.send("encrypted")
      assert_equal ["encrypted"], server.receive

      server.send("reply")
      assert_equal ["reply"], client.receive
    ensure
      client&.close
      server&.close
    end
  end


  private


  def build_self_signed_tls_context
    require "openssl"

    key  = OpenSSL::PKey::RSA.new(2048)
    name = OpenSSL::X509::Name.parse("/CN=127.0.0.1")
    cert = OpenSSL::X509::Certificate.new
    cert.version    = 2
    cert.serial     = 1
    cert.subject    = name
    cert.issuer     = name
    cert.public_key = key.public_key
    cert.not_before = Time.now - 60
    cert.not_after  = Time.now + 3600
    cert.sign(key, OpenSSL::Digest.new("SHA256"))

    ctx = OpenSSL::SSL::SSLContext.new
    ctx.cert        = cert
    ctx.key         = key
    ctx.min_version = OpenSSL::SSL::TLS1_2_VERSION
    ctx
  end
end
