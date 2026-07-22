# frozen_string_literal: true

require_relative "../test_helper"

describe "QoS transport connection class" do
  FakeConnection = Class.new do
    attr_reader :options
    attr_reader :peer_properties

    def initialize(_io, **options)
      @options = options
      @peer_properties = {}
    end


    def handshake!
    end


    def close
    end
  end


  FakeTransport = Module.new do
    def self.connection_class
      FakeConnection
    end
  end


  FakeRouting = Class.new do
    attr_reader :connection

    def connection_added(connection)
      @connection = connection
    end


    def connection_removed(_connection)
    end
  end


  FakeEngine = Class.new do
    attr_reader :socket_type, :options, :connections, :barrier, :routing,
                :peer_connected

    def initialize(parent)
      @socket_type = :PULL
      @options = OMQ::Options.new
      @connections = {}
      @barrier = Async::Barrier.new(parent: parent)
      @routing = FakeRouting.new
      @peer_connected = Async::Promise.new
    end


    def connection_wrapper
      nil
    end


    def transport_object_for(_endpoint)
      nil
    end


    def emit_monitor_event(*)
    end


    def maybe_resolve_all_peers_gone
    end


    def maybe_reconnect(_endpoint)
    end
  end


  it "preserves a transport-provided connection class" do
    Async do |task|
      engine = FakeEngine.new(task)
      lifecycle = OMQ::Engine::ConnectionLifecycle.new(engine, transport: FakeTransport)

      conn = lifecycle.handshake!(Object.new, as_server: true)

      assert_instance_of FakeConnection, conn
      assert_same conn, engine.routing.connection
    end
  end
end
