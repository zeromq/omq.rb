# frozen_string_literal: true

require_relative "../../test_helper"
require "timeout"

# These tests verify that CLI runners work correctly when the connecting
# side starts before the binding side.  This is the normal reconnect
# scenario: the library retries in the background until the peer appears.
#
# Each test spawns a runner thread (connect side), waits until that
# runner calls #connect, then starts the bind side. If the connect side
# hangs instead of reconnecting, the per-test timeout will catch it.
#
# Historically this broke when a handshake failure during the initial
# TCP connect killed the reconnect loop (v0.17.2 fix).

TEST_TIMEOUT = ENV.fetch("OMQ_TEST_TIMEOUT", "30").to_f


def spawn_runner(runner_class, socket_class, config)
  Thread.new do
    quietly do
      Sync do |task|
        runner_class.new(config, socket_class).call(task)
      end
    end
  end
end


def signaling_connect_class(socket_class, signal)
  Class.new(socket_class) do
    define_method(:connect) do |*args, **kwargs, &block|
      signal << true
      super(*args, **kwargs, &block)
    end
  end
end


def wait_for_connect_call(signal)
  Timeout.timeout(TEST_TIMEOUT) { signal.pop }
end


def make_cfg(type_name:, binds: [], connects: [], **overrides)
  make_config(
    type_name:     type_name,
    endpoints:     binds.map { |u| OMQ::CLI::Endpoint.new(u, true) } +
                   connects.map { |u| OMQ::CLI::Endpoint.new(u, false) },
    binds:         binds,
    connects:      connects,
    quiet:         true,
    count:         1,
    linger:        1,
    reconnect_ivl: 0.01,
    **overrides,
  )
end


def reserve_tcp_port
  server = TCPServer.new("127.0.0.1", 0)
  port   = server.local_address.ip_port
  server.close
  port
end


# ---------------------------------------------------------------------------
# connect before bind — the binding peer starts after the connecting peer
# ---------------------------------------------------------------------------

describe "connect before bind" do
  it "REQ -c then REP -b over TCP" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        port = reserve_tcp_port
        url  = "tcp://127.0.0.1:#{port}"

        req_cfg = make_cfg(type_name: "req", connects: [url], data: "ping")
        connected = Thread::Queue.new
        req_thread = spawn_runner(OMQ::CLI::ReqRunner, signaling_connect_class(OMQ::REQ, connected), req_cfg)

        wait_for_connect_call(connected)

        rep = OMQ::REP.new(linger: 1)
        rep.bind(url)
        msg = task.with_timeout(TEST_TIMEOUT - 1) { rep.receive }
        assert_equal ["ping"], msg
        rep.send(["pong"])

        req_thread.join
      ensure
        rep&.close
      end
    end
  end


  it "PUSH -c then PULL -b over TCP" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        port = reserve_tcp_port
        url  = "tcp://127.0.0.1:#{port}"

        push_cfg = make_cfg(type_name: "push", connects: [url], data: "hello")
        connected = Thread::Queue.new
        push_thread = spawn_runner(OMQ::CLI::PushRunner, signaling_connect_class(OMQ::PUSH, connected), push_cfg)

        wait_for_connect_call(connected)

        pull = OMQ::PULL.new
        pull.bind(url)
        msg = task.with_timeout(TEST_TIMEOUT - 1) { pull.receive }
        assert_equal ["hello"], msg

        push_thread.join
      ensure
        pull&.close
      end
    end
  end


  it "DEALER -c then ROUTER -b over TCP" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        port = reserve_tcp_port
        url  = "tcp://127.0.0.1:#{port}"

        dealer_cfg = make_cfg(type_name: "dealer", connects: [url], data: "hello")
        connected = Thread::Queue.new
        dealer_thread = spawn_runner(OMQ::CLI::PairRunner, signaling_connect_class(OMQ::DEALER, connected), dealer_cfg)

        wait_for_connect_call(connected)

        router = OMQ::ROUTER.new(linger: 1)
        router.bind(url)
        msg = task.with_timeout(TEST_TIMEOUT - 1) { router.receive }
        assert_equal "hello", msg.last

        dealer_thread.join
      ensure
        router&.close
      end
    end
  end


  it "PAIR -c then PAIR -b over TCP" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        port = reserve_tcp_port
        url  = "tcp://127.0.0.1:#{port}"

        pair_cfg = make_cfg(type_name: "pair", connects: [url], data: "hello")
        connected = Thread::Queue.new
        pair_thread = spawn_runner(OMQ::CLI::PairRunner, signaling_connect_class(OMQ::PAIR, connected), pair_cfg)

        wait_for_connect_call(connected)

        peer = OMQ::PAIR.new(linger: 1)
        peer.bind(url)
        msg = task.with_timeout(TEST_TIMEOUT - 1) { peer.receive }
        assert_equal ["hello"], msg

        pair_thread.join
      ensure
        peer&.close
      end
    end
  end
end


# ---------------------------------------------------------------------------
# Reconnect after handshake failure — exercises the v0.17.2 bug fix.
# A raw server accepts the TCP connection and immediately RSTs it
# (LINGER 0 close), killing the ZMTP handshake mid-flight.  Then a
# real OMQ server appears on the same port.  The connecting runner
# must recover and deliver its message.
# ---------------------------------------------------------------------------

describe "reconnect after handshake RST" do
  it "REQ recovers from a mid-handshake RST and reaches the real REP" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        # Raw server that accepts one connection and immediately RSTs it.
        raw = TCPServer.new("127.0.0.1", 0)
        port = raw.local_address.ip_port
        url  = "tcp://127.0.0.1:#{port}"

        resetter = Async do
          client = raw.accept
          client.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
          client.close
        end

        req_cfg = make_cfg(type_name: "req", connects: [url], data: "hello")
        req_thread = spawn_runner(OMQ::CLI::ReqRunner, OMQ::REQ, req_cfg)

        resetter.wait
        raw.close

        rep = OMQ::REP.new(linger: 1)
        rep.bind(url)

        msg = task.with_timeout(TEST_TIMEOUT - 1) { rep.receive }
        assert_equal ["hello"], msg
        rep.send(["reply"])

        req_thread.join
      ensure
        raw&.close rescue nil
        rep&.close
      end
    end
  end


  it "PUSH recovers from a mid-handshake RST and reaches the real PULL" do
    Sync do |task|
      task.with_timeout(TEST_TIMEOUT) do
        raw = TCPServer.new("127.0.0.1", 0)
        port = raw.local_address.ip_port
        url  = "tcp://127.0.0.1:#{port}"

        resetter = Async do
          client = raw.accept
          client.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
          client.close
        end

        push_cfg = make_cfg(type_name: "push", connects: [url], data: "hello")
        push_thread = spawn_runner(OMQ::CLI::PushRunner, OMQ::PUSH, push_cfg)

        resetter.wait
        raw.close

        pull = OMQ::PULL.new
        pull.bind(url)

        msg = task.with_timeout(TEST_TIMEOUT - 1) { pull.receive }
        assert_equal ["hello"], msg

        push_thread.join
      ensure
        raw&.close rescue nil
        pull&.close
      end
    end
  end
end
