# frozen_string_literal: true

require "async"
require "uri"
require_relative "engine/recv_pump"
require_relative "engine/heartbeat"
require_relative "engine/reconnect"
require_relative "engine/connection_lifecycle"
require_relative "engine/socket_lifecycle"
require_relative "engine/maintenance"
require_relative "engine/endpoints"
require_relative "engine/monitoring"
require_relative "engine/pump_tasks"

module OMQ
  # Per-socket orchestrator.
  #
  # Manages connections, transports, and the routing strategy for one
  # OMQ::Socket instance. Each socket type creates one Engine.
  #
  class Engine
    include Endpoints
    include Monitoring
    include PumpTasks


    # Scheme → transport module registry.
    # Plugins add entries via +Engine.transports["scheme"] = MyTransport+.
    #
    @transports = {}


    class << self
      # @return [Hash{String => Module}] registered transports
      attr_reader :transports


      # The URI {#bind} and {#connect} return for +endpoint+. A transport
      # with +.endpoint_uri+ builds it: IPC keeps the @ of abstract names.
      # The native backends use it for the endpoints they resolve.
      #
      # @param endpoint [String]
      # @return [URI::Generic]
      #
      def endpoint_uri(endpoint)
        transport = @transports[endpoint[/\A[^:]+/]]
        return transport.endpoint_uri(endpoint) if transport.respond_to?(:endpoint_uri)

        URI.parse(endpoint)
      end
    end


    # @return [Symbol] socket type (e.g. :REQ, :PAIR)
    #
    attr_reader :socket_type


    # @return [Options] socket options
    #
    attr_reader :options


    # @return [Hash{String => Listener}] active listeners keyed by resolved endpoint
    #
    attr_reader :listeners


    # @return [Hash{Connection => ConnectionLifecycle}] active connections
    #
    attr_reader :connections


    # Optional proc that wraps new connections (e.g. for serialization).
    # Called with the raw connection; must return the (possibly wrapped) connection.
    #
    attr_accessor :connection_wrapper


    # @return [SocketLifecycle] socket-level state + signaling
    #
    attr_reader :lifecycle


    # @!attribute [w] monitor_queue
    #   @param value [MonitorQueue, nil] queue for monitor events
    #
    attr_writer :monitor_queue


    # @return [Boolean] when true, every monitor event is also printed
    #   to stderr for debugging. Set via {Socket#monitor}.
    attr_accessor :verbose_monitor


    # @return [Routing] routing strategy (created lazily on first access)
    #
    def routing
      @routing ||= Routing.for(@socket_type).new(self)
    end


    # @param socket_type [Symbol] e.g. :REQ, :REP, :PAIR
    # @param options [Options]
    #
    def initialize(socket_type, options)
      @socket_type     = socket_type
      @options         = options
      @routing         = nil
      @connections     = {} # connection => ConnectionLifecycle
      @dialers         = {} # endpoint => Dialer (reconnect intent + connect logic)
      @dialer_barriers = {} # endpoint => Async::Barrier (its reconnect + connection tasks)
      @listeners       = {} # endpoint => Listener
      @bind_aliases    = {} # endpoint as passed to #bind => listener endpoint
      @lifecycle       = SocketLifecycle.new
      @fatal_error     = nil
      @monitor_queue   = nil
      @verbose_monitor = false
      @drain_signal    = nil # set while #close waits for send queues
      @batches_sending = 0   # batches pumps took off a queue and haven't flushed
    end


    # @return [Async::Promise] resolves with the first peer that connects
    #
    def peer_connected
      @lifecycle.peer_connected
    end


    # @return [Async::Promise] resolves once every peer is gone
    #
    def all_peers_gone
      @lifecycle.all_peers_gone
    end


    # @return [Async::Task, nil] the task the socket's tasks run under
    #
    def parent_task
      @lifecycle.parent_task
    end


    # @return [Async::Barrier, nil] the socket-level barrier
    #
    def barrier
      @lifecycle.barrier
    end


    # @return [Boolean] true once the socket is closed
    #
    def closed?
      @lifecycle.closed?
    end


    # @return [Boolean] true if the socket runs on the shared IO thread
    #
    def on_io_thread?
      @lifecycle.on_io_thread
    end


    # Enables or disables auto-reconnect for dropped connections.
    # Delegated to {SocketLifecycle}. Close paths flip this to +false+
    # so a lost connection doesn't schedule a new retry after linger.
    #
    # @param value [Boolean]
    #
    def reconnect_enabled=(value)
      @lifecycle.reconnect_enabled = value
    end


    # Delegated to the routing strategy. Forwards the PUB/XPUB/SUB/XSUB
    # subscription surface so callers don't have to chain through
    # `engine.routing`.
    #
    def subscriber_joined
      routing.subscriber_joined
    end


    # Subscribes to a topic prefix on SUB/XSUB sockets. Delegates to the
    # routing strategy so callers don't have to chain through
    # `engine.routing`.
    #
    # @param prefix [String]
    #
    def subscribe(prefix)
      routing.subscribe(prefix)
    end


    # Unsubscribes from a topic prefix on SUB/XSUB sockets. Delegates to
    # the routing strategy so callers don't have to chain through
    # `engine.routing`.
    #
    # @param prefix [String]
    #
    def unsubscribe(prefix)
      routing.unsubscribe(prefix)
    end


    # Records the disconnect reason on the {ConnectionLifecycle} for
    # +conn+, if any. Called by the recv pump rescue so the upcoming
    # `:disconnected` monitor event carries an error reason, without
    # exposing the internal connection map.
    #
    def record_disconnect_reason(conn, error)
      @connections[conn]&.record_disconnect_reason(error)
    end


    # Binds to an endpoint.
    #
    # @param endpoint [String] e.g. "tcp://127.0.0.1:5555", "ruby://foo"
    # @return [URI::Generic] resolved endpoint URI (with auto-selected port for "tcp://host:0")
    # @raise [ArgumentError] on unsupported transport
    #
    def bind(endpoint, parent: nil, **opts)
      OMQ.freeze_for_ractors!
      capture_parent_task(parent: parent)
      endpoint, transport = resolve_endpoint(endpoint)
      listener            = transport.listener(endpoint, self, **opts)

      start_accept_loops(listener)

      @listeners[listener.endpoint] = listener
      remember_bind_alias(endpoint, listener.endpoint)
      emit_monitor_event(:listening, endpoint: listener.endpoint)
      Engine.endpoint_uri(listener.endpoint)
    rescue => error
      emit_monitor_event(:bind_failed, endpoint: endpoint, detail: { error: error })
      raise
    end


    # Connects to an endpoint.
    #
    # @param endpoint [String]
    # @return [URI::Generic] parsed endpoint URI
    #
    def connect(endpoint, parent: nil, **opts)
      OMQ.freeze_for_ractors!
      capture_parent_task(parent: parent)
      endpoint, transport = resolve_endpoint(endpoint)
      validate_endpoint!(endpoint, transport)

      dialer             = transport.dialer(endpoint, self, **opts)
      @dialers[endpoint] = dialer

      if endpoint.start_with?("ruby://")
        connect_inproc(endpoint, dialer)
      else
        emit_monitor_event(:connect_delayed, endpoint: endpoint)
        schedule_reconnect(endpoint, delay: 0)
      end

      Engine.endpoint_uri(endpoint)
    end


    # Disconnects from an endpoint. Stops its reconnect loop and any
    # handshake in progress, then closes its connections.
    #
    # @param endpoint [String]
    # @return [void]
    #
    def disconnect(endpoint)
      endpoint = canonical_endpoint(endpoint)
      @dialers.delete(endpoint)
      @dialer_barriers.delete(endpoint)&.stop
      close_connections_at(endpoint)
      drop_route(endpoint)
    end


    # Unbinds from an endpoint. Stops the listener and closes all
    # connections that were accepted on it. Accepts the resolved
    # endpoint (as returned by {#bind}) or the string passed to
    # {#bind}, e.g. +tcp://localhost:5555+.
    #
    # @param endpoint [String]
    # @return [void]
    #
    def unbind(endpoint)
      endpoint = canonical_endpoint(endpoint)
      endpoint = @bind_aliases.fetch(endpoint, endpoint)
      listener = @listeners.delete(endpoint) or return

      @bind_aliases.delete_if { |_, bound| bound == endpoint }
      listener.stop
      close_connections_at(endpoint)
    end


    # Called by a transport when an incoming connection is accepted.
    #
    # @param io [#read, #write, #close]
    # @param endpoint [String, nil] the endpoint this was accepted on
    # @return [void]
    #
    def handle_accepted(io, endpoint: nil)
      emit_monitor_event(:accepted, endpoint: endpoint)
      spawn_connection(io, as_server: true, endpoint: endpoint)
    end


    # Called by a transport when an outgoing connection is established.
    #
    # @param io [#read, #write, #close]
    # @return [void]
    #
    def handle_connected(io, endpoint: nil)
      barrier = @dialer_barriers[endpoint]

      # Disconnected (or closed) while the dial was in flight.
      unless barrier && @dialers.key?(endpoint)
        io.close rescue nil
        return
      end

      emit_monitor_event(:connected, endpoint: endpoint)
      spawn_connection(io, as_server: false, endpoint: endpoint, parent: barrier)
    end


    # Called by inproc transport with a pre-validated Pipe.
    # Skips ZMTP handshake — just registers with routing strategy.
    #
    # @param pipe [Transport::Inproc::Pipe]
    # @return [Boolean] false if the routing strategy refused the peer
    #
    def connection_ready(pipe, endpoint: nil, dialed: false)
      ConnectionLifecycle.new(self, endpoint: endpoint, dialed: dialed).ready_direct!(pipe)
    end


    # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
    # @return [String, nil] the endpoint this socket dialed to get +conn+,
    #   nil for an accepted connection
    #
    def dialed_endpoint(conn)
      lifecycle = @connections[conn]
      lifecycle.endpoint if lifecycle&.dialed?
    end


    # @param endpoint [String, nil]
    # @return [Boolean] true if a lost connection to +endpoint+ gets
    #   redialed
    #
    def redials?(endpoint)
      return false unless endpoint && @dialers.key?(endpoint)

      @lifecycle.open? && @lifecycle.reconnect_enabled
    end


    # Drops the send queue a routing strategy keeps for a dialed
    # endpoint between connections (ROUTER, SERVER, PEER).
    #
    # @param endpoint [String, nil]
    # @return [void]
    #
    def drop_route(endpoint)
      @routing.drop_route(endpoint) if endpoint && @routing.respond_to?(:drop_route)
      wake_drain
    end


    # Wakes {#drain_send_queues} to check the send queues again. Send
    # pumps call it after each batch, teardown when a queue goes away.
    # A no-op unless {#close} is waiting.
    #
    # @return [void]
    #
    def wake_drain
      @drain_signal&.signal
    end


    # Runs a send pump's write of a batch it took off a queue. Until the
    # block returns, {#drain_send_queues} counts the batch as unsent, so
    # #close doesn't tear the connection down under the write.
    #
    # @yield the write and flush
    # @return [Object] the block's value
    #
    def sending_batch
      @batches_sending += 1
      yield
    ensure
      @batches_sending -= 1
      wake_drain
    end


    # Dequeues the next received message. Blocks until available.
    #
    # @return [Array<String>, nil] message parts, or nil once the socket
    #   is closed (like +Thread::Queue#pop+ on a closed queue)
    # @raise if a background pump task crashed
    #
    def dequeue_recv
      raise @fatal_error if @fatal_error
      return if @lifecycle.closed?

      msg = routing.dequeue_recv
      raise @fatal_error if msg.nil? && @fatal_error

      msg
    end


    # Pushes a nil sentinel into the recv queue, unblocking a
    # pending {#dequeue_recv} with a nil return value.
    #
    def dequeue_recv_sentinel
      routing.unblock_recv
    end


    # Enqueues a message for sending. Blocks at HWM.
    #
    # @param parts [Array<String>]
    # @return [void]
    # @raise if a background pump task crashed
    # @raise [SocketClosedError] if the socket is closing or closed,
    #   including while blocked at HWM
    #
    def enqueue_send(parts)
      raise @fatal_error if @fatal_error
      raise_if_closed!

      routing.enqueue(parts)
    rescue Async::Queue::ClosedError, ClosedQueueError
      raise SocketClosedError, "#{@socket_type} socket closed while sending"
    end


    # Starts a recv pump for a connection, or wires the inproc fast path.
    #
    # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
    # @param recv_queue [Async::LimitedQueue]
    # @yield [msg] optional per-message transform
    # @return [Async::Task, nil]
    #
    def start_recv_pump(conn, recv_queue, &transform)
      # Spawn on the connection's lifecycle barrier so the recv pump is
      # torn down together with the rest of its sibling per-connection
      # pumps when the connection is lost.
      parent = @connections[conn]&.barrier || @lifecycle.barrier

      RecvPump.start(parent, conn, recv_queue, self, transform)
    end


    # Called when a connection is lost.
    #
    # @param connection [Protocol::ZMTP::Connection]
    # @return [void]
    #
    def connection_lost(connection)
      @connections[connection]&.lost!
    end


    # Resolves `all_peers_gone` if we had peers and now have none.
    # Called by ConnectionLifecycle during teardown.
    #
    def maybe_resolve_all_peers_gone
      @lifecycle.maybe_resolve_all_peers_gone(@connections)
    end


    # Schedules a reconnect for +endpoint+ if auto-reconnect is enabled
    # and the endpoint is still dialed.
    #
    def maybe_reconnect(endpoint)
      return unless redials?(endpoint)

      Reconnect.schedule(@dialers[endpoint], @options, dialer_barrier(endpoint), self)
    end


    # Closes all connections and listeners gracefully. Drains pending
    # sends up to +linger+ seconds, then cascades teardown through the
    # socket-level {SocketLifecycle#barrier} — every per-connection
    # barrier is stopped as a side effect, cancelling every pump.
    #
    # @return [void]
    #
    def close
      return close_unopened if @lifecycle.state == :new
      return unless @lifecycle.open?

      @lifecycle.start_closing!
      stop_listeners unless @connections.empty?

      if @options.linger.nil? || @options.linger > 0
        drain_send_queues(@options.linger)
      end

      @lifecycle.finish_closing!

      if @lifecycle.on_io_thread
        Reactor.untrack_linger(@options.linger)
      end

      stop_listeners
      tear_down_barrier
      close_routing_queues
      emit_monitor_event(:closed)
      close_monitor_queue
    end


    # Immediate hard stop: skips the linger drain and cascades teardown
    # through the socket-level barrier. Intended for crash-path cleanup
    # where {#close}'s drain is either unsafe or undesired.
    #
    # @return [void]
    #
    def stop
      return unless @lifecycle.alive?

      @lifecycle.force_close!

      if @lifecycle.on_io_thread
        Reactor.untrack_linger(@options.linger)
      end

      stop_listeners
      tear_down_barrier
      close_routing_queues
      emit_monitor_event(:closed)
      close_monitor_queue
    end


    # Captures the socket's task tree root and starts the socket-level
    # maintenance task. If +parent+ is given, it is used as the parent
    # for every task spawned under this socket (connection supervisors,
    # reconnect loops, maintenance, monitor). Otherwise the current
    # Async task (or the shared Reactor root, for non-Async callers)
    # is captured automatically.
    #
    # Idempotent: first call wins. Subsequent calls (including from
    # later bind/connect invocations) with a different +parent+ are
    # silently ignored.
    #
    # @param parent [#async, nil] optional Async parent
    #
    def capture_parent_task(parent: nil)
      raise_if_closed!

      task = @lifecycle.capture_parent_task(parent: parent, linger: @options.linger)

      return unless task

      Maintenance.start(@lifecycle.barrier, @options.mechanism)
    end


    private


    # Spawns a per-connection task on the socket-level barrier that runs
    # the handshake and then blocks on +done+ until the connection is
    # torn down. The +ensure+ path runs {ConnectionLifecycle#close!} so
    # side effects (routing removal, monitor event, reconnect) always
    # fire exactly once, regardless of how the task unwinds.
    #
    # @param io [#read, #write, #close] accepted or dialed socket
    # @param as_server [Boolean] true for accepted connections
    # @param endpoint [String, nil] the endpoint URI, for monitor events
    # @param parent [Async::Barrier, nil] owner of the connection task:
    #   the dialer's barrier for dialed connections, so {#disconnect}
    #   also stops a handshake in progress
    #
    def spawn_connection(io, as_server:, endpoint: nil, parent: @lifecycle.barrier)
      parent&.async(transient: true, annotation: "conn #{endpoint}") do
        done      = Async::Promise.new
        transport = endpoint ? transport_for(endpoint) : nil
        lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint, done: done, transport: transport, dialed: !as_server)
        lifecycle.handshake!(io, as_server: as_server)
        done.wait
      rescue Async::Stop, Async::Cancel
        # socket barrier stopped — cascade teardown
      rescue Async::Queue::ClosedError
        # connection dropped during drain — message re-staged
      rescue Protocol::ZMTP::Error, *CONNECTION_LOST, Async::TimeoutError
        # handshake failed, connection lost, or handshake timed out
      ensure
        lifecycle&.close!
      end
    end


    # Blocks until the routing strategy reports all send queues drained
    # or +timeout+ seconds have elapsed. Called from the linger path on
    # close so pending outgoing messages get a chance to flush. Sleeps
    # until {#wake_drain} says something changed.
    #
    # @param timeout [Numeric, nil] max seconds to wait; nil = no limit
    #
    def drain_send_queues(timeout)
      return unless @routing.respond_to?(:send_queues_drained?)

      deadline      = timeout&.finite? ? Async::Clock.now + timeout : nil # nil, Infinity: no limit
      @drain_signal = Async::Notification.new

      until @batches_sending.zero? && @routing.send_queues_drained?
        if deadline
          remaining = deadline - Async::Clock.now
          break if remaining <= 0

          Async::Task.current.with_timeout(remaining) { @drain_signal.wait }
        else
          @drain_signal.wait
        end
      end
    rescue Async::TimeoutError
      # linger expired
    rescue Async::Stop
      # Parent task is being cancelled — stop draining and let close
      # proceed with the rest of teardown instead of propagating the
      # cancellation out of the ensure path.
    ensure
      @drain_signal = nil
    end


    # Schedules a reconnect attempt for +endpoint+ using its dialer.
    # Called on initial connect (with +delay: 0+) and after a lost
    # connection (with the dialer's backoff delay).
    #
    # @param endpoint [String]
    # @param delay [Numeric, nil] initial delay override in seconds
    #
    def schedule_reconnect(endpoint, delay: nil)
      dialer = @dialers[endpoint]
      Reconnect.schedule(dialer, @options, dialer_barrier(endpoint), self, delay: delay)
    end


    # Returns the barrier that owns +endpoint+'s reconnect loop and
    # dialed connection tasks, nested in the socket barrier so
    # {#close} stops them too.
    #
    # @param endpoint [String]
    # @return [Async::Barrier]
    #
    def dialer_barrier(endpoint)
      @dialer_barriers[endpoint] ||= Async::Barrier.new(parent: @lifecycle.barrier)
    end


    # Ruby in-process connects are synchronous: link now, or wait up to
    # one reconnect interval for a bind, then keep retrying in the
    # background like any other dialer.
    #
    # @param endpoint [String]
    # @param dialer [Transport::Inproc::Dialer]
    #
    def connect_inproc(endpoint, dialer)
      dialer.connect(wait: true)
    rescue Errno::ECONNREFUSED
      emit_monitor_event(:connect_delayed, endpoint: endpoint)
      schedule_reconnect(endpoint)
    rescue
      @dialers.delete(endpoint)
      raise
    end


    # Starts the listener's accept loops on the socket-level barrier
    # and routes accepted IOs through {#handle_accepted}.
    #
    # @param listener [#start_accept_loops, #endpoint]
    #
    def start_accept_loops(listener)
      return unless listener.respond_to?(:start_accept_loops)

      listener.start_accept_loops(@lifecycle.barrier) do |io|
        handle_accepted(io, endpoint: listener.endpoint)
      end
    end


    # Stops every active listener and clears the registry. Called from
    # close paths; connection teardown is handled separately via the
    # socket-level barrier.
    #
    def stop_listeners
      @listeners.each_value(&:stop)
      @listeners.clear
      @bind_aliases.clear
    end


    # Lets {#unbind} find a listener by the string the caller passed to
    # {#bind}. Ephemeral-port requests (+:0+, +:*+) can bind many
    # listeners, so they don't get an alias.
    #
    # @param requested [String] canonical endpoint passed to #bind
    # @param bound [String] the listener's resolved endpoint
    #
    def remember_bind_alias(requested, bound)
      return if requested == bound
      return if requested.end_with?(":0", ":*")

      @bind_aliases[requested] = bound
    end


    # Closes all connections currently bound to or connected from
    # +endpoint+. Used by {#disconnect} and {#unbind}.
    #
    # @param endpoint [String]
    #
    def close_connections_at(endpoint)
      @connections.values.select { |lc| lc.endpoint == endpoint }.each(&:close!)
    end


    # Cascades teardown through the socket-level barrier. Stopping the
    # barrier cancels every tracked task: connection supervisors (whose
    # `ensure lost!` runs the ordered disconnect side effects), accept
    # loops, heartbeat, maintenance, and the per-endpoint dialer
    # barriers with their reconnect loops.
    #
    def tear_down_barrier
      @lifecycle.barrier&.stop
    end


    # @raise [SocketClosedError] once {#close} or {#stop} has started
    #
    def raise_if_closed!
      return if @lifecycle.alive?

      raise SocketClosedError, "#{@socket_type} socket is closed"
    end


    # Closes a socket that never bound or connected: there are no tasks
    # to stop, but later calls must see it closed.
    #
    def close_unopened
      @lifecycle.finish_closing!
      close_routing_queues
      emit_monitor_event(:closed)
      close_monitor_queue
    end


    # Wakes fibers blocked in a send (they raise {SocketClosedError}) or
    # a receive (they get +nil+).
    #
    def close_routing_queues
      @routing.close_queues if @routing.respond_to?(:close_queues)
    end

  end
end
