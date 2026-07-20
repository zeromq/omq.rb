# frozen_string_literal: true

require_relative "../test_helper"

# Broader scenarios for ws://: mechanism branches (ZWS2.0/NULL vs
# ZWS2.0 no-mechanism), identity-aware routing (ROUTER/DEALER),
# fan-in, multi-frame envelopes, large payloads, custom ws_path,
# CANCEL, and heartbeat over an idle connection.
describe "WebSocket integration" do


  it "ZWS2.0/NULL: identity propagates and ROUTER routes by it" do
    Async do
      router = OMQ::ROUTER.new
      port   = router.bind("ws://127.0.0.1:0").port

      dealer = OMQ::DEALER.new(identity: "alice")
      dealer.connect("ws://127.0.0.1:#{port}")

      dealer.send(["hello"])

      msg = router.receive
      assert_equal "alice", msg.first
      assert_equal "hello", msg.last

      router.send(["alice", "world"])
      assert_equal ["world"], dealer.receive
    ensure
      dealer&.close
      router&.close
    end
  end


  it "ZWS2.0 (no mechanism): identity-as-first-message round-trips on PAIR" do
    Async do
      no_mech = ["ZWS2.0"]

      server = OMQ::PAIR.new
      server.ws_subprotocols = no_mech
      port = server.bind("ws://127.0.0.1:0").port

      client = OMQ::PAIR.new
      client.ws_subprotocols = no_mech
      client.connect("ws://127.0.0.1:#{port}")

      client.send(["plain", "frames"])
      assert_equal ["plain", "frames"], server.receive

      server.send("reply")
      assert_equal ["reply"], client.receive
    ensure
      client&.close
      server&.close
    end
  end


  it "fan-in: three PUSH peers deliver to one PULL" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("ws://127.0.0.1:0").port

      pushes = 3.times.map do |i|
        push = OMQ::PUSH.new(identity: "p#{i}")
        push.connect("ws://127.0.0.1:#{port}")
        push
      end

      pushes.each_with_index { |p, i| p.send("hello-#{i}") }

      received = 3.times.map { pull.receive.first }.sort
      assert_equal %w[hello-0 hello-1 hello-2], received
    ensure
      pushes&.each(&:close)
      pull&.close
    end
  end


  it "transmits a five-frame multipart envelope intact" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("ws://127.0.0.1:0").port

      push = OMQ::PUSH.connect("ws://127.0.0.1:#{port}")

      parts = %w[a b c d e]
      push.send(parts)

      assert_equal parts, pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "transmits a 1 MiB payload across one WebSocket binary frame" do
    Async do
      pull = OMQ::PULL.new
      port = pull.bind("ws://127.0.0.1:0").port

      push = OMQ::PUSH.connect("ws://127.0.0.1:#{port}")

      payload = "x" * (1 << 20)
      push.send(payload)

      msg = pull.receive
      assert_equal 1, msg.size
      assert_equal payload.bytesize, msg.first.bytesize
      assert_equal payload, msg.first
    ensure
      push&.close
      pull&.close
    end
  end


  it "honors a non-default ws_path on both sides" do
    Async do
      pull = OMQ::PULL.new
      pull.ws_path = "/zmq"
      port = pull.bind("ws://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.ws_path = "/zmq"
      push.connect("ws://127.0.0.1:#{port}/zmq")

      push.send("routed")
      assert_equal ["routed"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "PUB/SUB: CANCEL stops further delivery" do
    Async do |task|
      pub  = OMQ::PUB.new
      port = pub.bind("ws://127.0.0.1:0").port

      sub = OMQ::SUB.new(subscribe: "topic-a")
      sub.connect("ws://127.0.0.1:#{port}")

      sleep 0.05

      pub.send(["topic-a", "first"])
      assert_equal ["topic-a", "first"], sub.receive

      sub.unsubscribe("topic-a")
      sleep 0.05

      # Probe both topics; subscribe to a fresh one so we know when the
      # CANCEL has been observed at the publisher.
      sub.subscribe("topic-c")
      sleep 0.05

      pub.send(["topic-a", "ignored"])
      pub.send(["topic-c", "delivered"])

      msg = sub.receive
      assert_equal ["topic-c", "delivered"], msg
    ensure
      sub&.close
      pub&.close
    end
  end


  it "heartbeat keeps an idle connection alive long enough to send later" do
    Async do |task|
      pull = OMQ::PULL.new
      pull.heartbeat_interval = 0.05
      pull.heartbeat_timeout  = 0.5
      port = pull.bind("ws://127.0.0.1:0").port

      push = OMQ::PUSH.new
      push.heartbeat_interval = 0.05
      push.heartbeat_timeout  = 0.5
      push.connect("ws://127.0.0.1:#{port}")

      # Idle period long enough for several PING/PONG round-trips
      # to fire, but well below either heartbeat_timeout.
      sleep 0.3

      push.send("still-here")
      assert_equal ["still-here"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end


  it "wrong path returns 404 and the listener keeps serving the right one" do
    Async do
      pull = OMQ::PULL.new
      pull.ws_path = "/zmq"
      port = pull.bind("ws://127.0.0.1:0").port

      require "async/http/client"
      require "async/http/endpoint"

      bad_endpoint = ::Async::HTTP::Endpoint.parse("http://127.0.0.1:#{port}/wrong")
      bad_client   = ::Async::HTTP::Client.new(bad_endpoint)
      bad_response = bad_client.get("/wrong")
      assert_equal 404, bad_response.status
      bad_response.finish
      bad_client.close

      push = OMQ::PUSH.new
      push.ws_path = "/zmq"
      push.connect("ws://127.0.0.1:#{port}/zmq")

      push.send("ok")
      assert_equal ["ok"], pull.receive
    ensure
      push&.close
      pull&.close
    end
  end

end
