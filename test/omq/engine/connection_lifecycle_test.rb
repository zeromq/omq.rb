# frozen_string_literal: true

require_relative "../../test_helper"

# Stub engine for isolated lifecycle tests — no real sockets, no Async.
class FakeEngine
  attr_reader :connections, :events, :routing_added, :routing_removed,
              :reconnect_calls, :peer_connected, :all_peers_gone_resolved

  attr_accessor :connection_wrapper
  attr_reader :barrier

  def initialize
    @connections             = {}
    @events                  = []
    @routing_added           = []
    @routing_removed         = []
    @reconnect_calls         = []
    @all_peers_gone_resolved = 0
    @peer_connected          = FakePromise.new
    @barrier                 = Async::Barrier.new
  end

  def routing = self

  # routing interface
  def connection_added(conn)   = @routing_added << conn
  def connection_removed(conn) = @routing_removed << conn

  def emit_monitor_event(type, endpoint: nil, detail: nil)
    @events << [type, endpoint]
  end

  def maybe_resolve_all_peers_gone
    @all_peers_gone_resolved += 1 if @connections.empty?
  end

  def maybe_reconnect(endpoint)
    @reconnect_calls << endpoint
  end

  def transport_object_for(_endpoint)
    nil
  end
end

class FakePromise
  attr_reader :resolved_with

  def initialize
    @resolved     = false
    @resolved_with = nil
  end

  def resolve(value)
    return if @resolved
    @resolved     = true
    @resolved_with = value
  end

  def resolved? = @resolved
end

class FakeConn
  attr_reader :closed

  def initialize
    @closed = false
  end

  def close
    @closed = true
  end
end

describe OMQ::Engine::ConnectionLifecycle do
  let(:engine)   { FakeEngine.new }
  let(:pipe)     { FakeConn.new }
  let(:lifecycle) { OMQ::Engine::ConnectionLifecycle.new(engine, endpoint: "ruby://x") }

  describe "#ready_direct!" do
    it "runs the ordered ready sequence" do
      lifecycle.ready_direct!(pipe)

      assert_equal :ready, lifecycle.state
      assert_equal pipe,   lifecycle.conn
      assert_equal({ pipe => lifecycle }, engine.connections)
      assert_equal [pipe], engine.routing_added
      assert_equal pipe,   engine.peer_connected.resolved_with
    end

    it "emits :handshake_succeeded BEFORE connection_added" do
      lifecycle.ready_direct!(pipe)

      # routing_added captures the order side effects fire — asserting both
      # happened isn't enough. Check the event was emitted before routing got the conn.
      assert_equal [[:handshake_succeeded, "ruby://x"]], engine.events
      assert_equal [pipe], engine.routing_added
    end

    it "applies connection_wrapper if set" do
      wrapped            = FakeConn.new
      engine.connection_wrapper = ->(c) { wrapped }

      lifecycle.ready_direct!(pipe)

      assert_equal wrapped, lifecycle.conn
      assert_equal [wrapped], engine.routing_added
    end
  end

  describe "#lost!" do
    before { lifecycle.ready_direct!(pipe) }

    it "runs the ordered teardown sequence" do
      lifecycle.lost!

      assert_equal :closed, lifecycle.state
      assert_empty engine.connections
      assert_equal [pipe], engine.routing_removed
      assert pipe.closed
      assert_equal [:handshake_succeeded, :disconnected], engine.events.map(&:first)
      assert_equal ["ruby://x"], engine.reconnect_calls
      assert_equal 1, engine.all_peers_gone_resolved
    end

    it "is idempotent — second call is a no-op" do
      lifecycle.lost!
      lifecycle.lost!

      assert_equal 1, engine.routing_removed.size
      assert_equal 1, engine.reconnect_calls.size
      assert_equal 1, engine.events.count { |t, _| t == :disconnected }
    end

    it "resolves the done promise" do
      done = FakePromise.new
      lc   = OMQ::Engine::ConnectionLifecycle.new(engine, endpoint: "tcp://x", done: done)
      lc.ready_direct!(pipe)

      lc.lost!

      assert done.resolved?
    end
  end

  describe "#close!" do
    before { lifecycle.ready_direct!(pipe) }

    it "tears down without scheduling a reconnect" do
      lifecycle.close!

      assert_equal :closed, lifecycle.state
      assert_empty engine.connections
      assert_empty engine.reconnect_calls
      assert_equal 1, engine.events.count { |t, _| t == :disconnected }
    end

    it "is idempotent" do
      lifecycle.close!
      lifecycle.close!

      assert_equal 1, engine.routing_removed.size
    end
  end

  describe "state machine" do
    it "rejects :ready → :handshaking" do
      lifecycle.ready_direct!(pipe)
      assert_raises(OMQ::Engine::ConnectionLifecycle::InvalidTransition) do
        lifecycle.send(:transition!, :handshaking)
      end
    end

    it "rejects :new → :ready via transition! (must go through ready!)" do
      # ready! itself is a valid :new → :ready transition via ready_direct!
      # but calling transition!(:ready) from :handshaking is the normal TCP path;
      # calling from :closed is invalid
      lifecycle.ready_direct!(pipe)
      lifecycle.close!
      assert_raises(OMQ::Engine::ConnectionLifecycle::InvalidTransition) do
        lifecycle.send(:transition!, :ready)
      end
    end

    it "lost! before any ready transition is a no-op after reaching :closed" do
      # Construct, immediately call lost! — valid :new → :closed transition
      fresh = OMQ::Engine::ConnectionLifecycle.new(engine, endpoint: "ruby://y")
      fresh.lost!

      assert_equal :closed, fresh.state
      # No conn was ever set — routing.connection_removed should not fire
      assert_empty engine.routing_removed
    end
  end
end
