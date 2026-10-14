# frozen_string_literal: true

module OMQ
  class Engine
    # Owns the full arc of *one* connection: handshake → ready → closed.
    #
    # Scope boundary: ConnectionLifecycle tracks a single peer link
    # (one ZMTP connection or one inproc Pipe). SocketLifecycle
    # owns the socket-wide state above it — first-peer/last-peer
    # signaling, reconnect enable flag, the parent task tree, and the
    # open → closing → closed transitions that gate close-time drain.
    # A socket has exactly one SocketLifecycle and zero-or-more
    # ConnectionLifecycles beneath it.
    #
    # Centralizes the ordering of side effects (monitor events, routing
    # registration, promise resolution, reconnect scheduling) so the
    # sequence lives in one place instead of being scattered across
    # Engine, ConnectionSetup, and close paths.
    #
    # State machine:
    #
    #   new ──┬── :handshaking ── :ready ── :closed
    #         └── :ready ── :closed            (inproc fast path)
    #
    # #lost! and #close! are idempotent — the state guard ensures side
    # effects run exactly once even if multiple pumps race to report a
    # lost connection.
    #
    class ConnectionLifecycle

      class InvalidTransition < RuntimeError
      end


      STATES = %i[new handshaking ready closed].freeze


      TRANSITIONS = {
        new:         %i[handshaking ready closed].freeze,
        handshaking: %i[ready closed].freeze,
        ready:       %i[closed].freeze,
        closed:      [].freeze,
      }.freeze


      # @return [Protocol::ZMTP::Connection, Transport::Inproc::Pipe, nil]
      attr_reader :conn


      # @return [String, nil]
      attr_reader :endpoint


      # @return [Symbol] current state
      attr_reader :state


      # @return [Async::Barrier] holds all per-connection pump tasks
      #   (send pump, recv pump, reaper, heartbeat). When the connection
      #   is torn down, {#tear_down!} calls `@barrier.stop` to take down
      #   every sibling task atomically — so the first pump to see a
      #   disconnect takes down all the others.
      attr_reader :barrier


      # @param engine [Engine]
      # @param endpoint [String, nil]
      # @param done [Async::Promise, nil] resolved when connection is lost
      # @param transport [Module, nil] transport module that produced +io+;
      #   queried for {.connection_class} so plugins (e.g. WebSocket) can
      #   substitute their own ZMTP-shaped connection class. Falls back to
      #   {Protocol::ZMTP::Connection} when nil or when the transport
      #   doesn't define +connection_class+.
      #
      def initialize(engine, endpoint: nil, done: nil, transport: nil)
        @engine    = engine
        @endpoint  = endpoint
        @done      = done
        @transport = transport
        @state     = :new
        @conn      = nil

        # Nest the per-connection barrier under the socket-level barrier
        # so every pump spawned via +@barrier.async+ is also tracked by
        # the socket barrier — {Engine#stop}/{Engine#close} cascade
        # through in one call.
        @barrier  = Async::Barrier.new(parent: engine.barrier)
      end


      # Performs the ZMTP handshake and transitions to :ready.
      #
      # @param io [#read, #write, #close]
      # @param as_server [Boolean]
      # @return [Protocol::ZMTP::Connection]
      #
      def handshake!(io, as_server:)
        transition!(:handshaking)
        conn_class = @transport.respond_to?(:connection_class) ? @transport.connection_class : Protocol::ZMTP::Connection
        conn = conn_class.new io,
          socket_type:      @engine.socket_type.to_s,
          identity:         @engine.options.identity,
          as_server:        as_server,
          mechanism:        @engine.options.mechanism&.dup,
          max_message_size: @engine.options.max_message_size

        Async::Task.current.with_timeout(handshake_timeout) do
          conn.handshake!
        end

        Heartbeat.start(@barrier, conn, @engine.options)
        ready!(conn)
        @conn
      rescue Protocol::ZMTP::Error, *CONNECTION_LOST, Async::TimeoutError => error
        @engine.emit_monitor_event :handshake_failed,
          endpoint: @endpoint, detail: { error: error }

        conn&.close

        # Reconnect after temporary failures. An explicit ERROR response
        # forbids retrying with the same credentials. Without tear-down,
        # spawn_connection's ensure-block close! skips maybe_reconnect,
        # leaving the endpoint dead. Race is exposed when a peer RSTs
        # mid-handshake (e.g. LINGER 0 close against an in-flight connect).
        tear_down!(reconnect: !error.is_a?(Protocol::ZMTP::FatalHandshakeError))
        raise
      end


      # Registers an already-connected inproc pipe as :ready.
      # No handshake — inproc Pipe bypasses ZMTP entirely.
      #
      # @param pipe [Transport::Inproc::Pipe]
      #
      def ready_direct!(pipe)
        if pipe.is_a?(Transport::Inproc::Pipe) && pipe.peer
          @barrier.async(transient: true, annotation: "inproc peer close") { pipe.peer.wait_closed }
        end
        ready!(pipe)
      end


      # Transitions to :closed, running the full loss sequence:
      # routing removal, monitor event, reconnect scheduling.
      # Idempotent: a no-op if already :closed.
      #
      def lost!(reason: nil)
        tear_down!(reconnect: true, reason: reason || @disconnect_reason)
      end


      # Records the exception that took down a pump task so that the
      # supervisor can surface it in the :disconnected monitor event.
      # First writer wins — subsequent pumps unwinding on the same
      # teardown don't overwrite the original cause.
      #
      def record_disconnect_reason(error)
        @disconnect_reason ||= error
      end


      # Transitions to :closed without scheduling a reconnect.
      # Used by shutdown paths (Engine#close, #disconnect, #unbind).
      # Idempotent.
      #
      def close!
        tear_down!(reconnect: false)
      end


      private


      def ready!(conn)
        if @engine.connection_wrapper
          conn = @engine.connection_wrapper.call(conn)
        end

        if @endpoint
          transport_obj = @engine.transport_object_for(@endpoint)
          if transport_obj.respond_to?(:wrap_connection)
            conn = transport_obj.wrap_connection(conn)
          end
        end

        @conn = conn
        @engine.connections[@conn] = self
        @engine.emit_monitor_event(:handshake_succeeded, endpoint: @endpoint)
        @engine.routing.connection_added(@conn)
        @engine.peer_connected.resolve(@conn)
        transition!(:ready)

        # Isolated unit tests can use a FakeEngine without pumps at all.
        # Waiting on an empty barrier returns immediately and would
        # tear the connection down right after registering.
        start_supervisor unless @barrier.empty?
      end


      # Spawns a supervisor task on the *socket-level* barrier (not the
      # per-connection barrier) that blocks on the first pump to finish
      # and then triggers teardown.
      #
      # Keeping the supervisor out of the per-connection barrier avoids
      # the self-stop problem: stopping the current task raises
      # Async::Cancel synchronously and unwinds before side effects can
      # run. Placing it on the socket barrier means {Engine#stop} /
      # {Engine#close} cascade-cancels the supervisor, whose +ensure+
      # runs the ordered disconnect side effects once.
      #
      def start_supervisor
        @supervisor = @engine.barrier.async(transient: true, annotation: "conn supervisor") do
          @barrier.wait do |task|
            task.wait
            break
          end
        rescue Async::Stop, Async::Cancel
          # socket or supervisor cancelled externally (socket closing)
        ensure
          lost!
        end
      end


      def tear_down!(reconnect:, reason: nil)
        return if @state == :closed

        transition!(:closed)
        @engine.connections.delete(@conn)
        @engine.routing.connection_removed(@conn) if @conn
        @conn&.close rescue nil
        detail = reason ? { error: reason, reason: reason.message } : nil
        @engine.emit_monitor_event(:disconnected, endpoint: @endpoint, detail: detail)
        @done&.resolve(true)
        @engine.maybe_resolve_all_peers_gone
        @engine.maybe_reconnect(@endpoint) if reconnect

        # Cancel every sibling pump of this connection. The caller is
        # the supervisor task, which is NOT in the barrier — so there
        # is no self-stop risk.
        @barrier.stop
      end


      # Handshake timeout: same logic as TCP.connect_timeout — derived
      # from reconnect_interval (floor 0.5s). Prevents a hang when the
      # peer accepts the TCP connection but never sends a ZMTP greeting
      # (e.g. a non-ZMQ service on the same port).
      #
      def handshake_timeout
        ri = @engine.options.reconnect_interval
        ri = ri.end if ri.is_a?(Range)
        [ri, 0.5].max
      end


      def transition!(new_state)
        allowed = TRANSITIONS[@state]

        unless allowed&.include?(new_state)
          raise InvalidTransition, "#{@state} → #{new_state}"
        end

        @state = new_state
      end

    end
  end
end
