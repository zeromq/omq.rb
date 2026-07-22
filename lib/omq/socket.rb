# frozen_string_literal: true

require "forwardable"

module OMQ
  # Socket base class.
  #
  class Socket
    extend Forwardable

    # Creates a new socket and binds it to the given endpoint.
    #
    # @param endpoint [String]
    # @param opts [Hash] keyword arguments forwarded to {#initialize}
    # @return [Socket]
    #
    def self.bind(endpoint, **opts)
      new("@#{endpoint}", **opts)
    end


    # Creates a new socket and connects it to the given endpoint.
    #
    # @param endpoint [String]
    # @param opts [Hash] keyword arguments forwarded to {#initialize}
    # @return [Socket]
    #
    def self.connect(endpoint, **opts)
      new(">#{endpoint}", **opts)
    end


    # @return [Options]
    #
    attr_reader :options


    # @return [Engine] the socket's engine. Exposed for peer tooling
    #   (omq-cli, omq-ractor) that needs to reach into the
    #   socket's internals — not part of the stable user API.
    #
    attr_reader :engine


    # Delegate socket option accessors to @options.
    #
    def_delegators :@options,
      :send_hwm,              :send_hwm=,
      :recv_hwm,              :recv_hwm=,
      :linger,                :linger=,
      :identity,              :identity=,
      :recv_timeout,          :recv_timeout=,
      :send_timeout,          :send_timeout=,
      :read_timeout,          :read_timeout=,
      :write_timeout,         :write_timeout=,
      :router_mandatory,      :router_mandatory=,
      :router_mandatory?,
      :reconnect_interval,    :reconnect_interval=,
      :heartbeat_interval,    :heartbeat_interval=,
      :heartbeat_ttl,         :heartbeat_ttl=,
      :heartbeat_timeout,     :heartbeat_timeout=,
      :max_message_size,      :max_message_size=,
      :sndbuf,                :sndbuf=,
      :rcvbuf,                :rcvbuf=,
      :on_mute,               :on_mute=,
      :mechanism,             :mechanism=


    # @param endpoints [String, nil] optional endpoint with prefix convention
    #   (+@+ for bind, +>+ for connect, plain uses subclass default)
    #
    # @yieldparam [self] the socket, when a block is passed; the socket
    #   is {#close}d when the block returns (or raises).
    #
    # Use option accessors (e.g. +socket.linger = 0+) to configure
    # post-construction.
    #
    def initialize(endpoints = nil)
    end


    # Yields +self+ if a block was given, then closes. Called by every
    # subclass initializer so +OMQ::PUSH.new { |p| ... }+ behaves like
    # +File.open+: configure, use, auto-close.
    #
    def finalize_init
      return unless block_given?
      begin
        yield self
      ensure
        close
      end
    end


    # Binds to an endpoint.
    #
    # @param endpoint [String]
    # @param parent [#async, nil] Async parent for the socket's task tree.
    #   Accepts any object that responds to +#async+ — +Async::Task+,
    #   +Async::Barrier+, +Async::Semaphore+. When given, every task
    #   spawned under this socket (connection supervisors, reconnect
    #   loops, heartbeat, monitor) is placed under +parent+, so callers
    #   can coordinate teardown with their own Async tree. Only the
    #   *first* bind/connect call captures the parent — subsequent
    #   calls ignore the kwarg.
    # @return [URI::Generic] resolved endpoint URI (with auto-selected port for "tcp://host:0")
    #
    def bind(endpoint, parent: nil, **opts)
      ensure_parent_task(parent: parent)
      Reactor.run { @engine.bind(endpoint, **opts) } # TODO: use timeout?
    end


    # Connects to an endpoint.
    #
    # @param endpoint [String]
    # @param parent [#async, nil] see {#bind}.
    # @return [URI::Generic] parsed endpoint URI
    #
    def connect(endpoint, parent: nil, **opts)
      ensure_parent_task(parent: parent)
      Reactor.run { @engine.connect(endpoint, **opts) } # TODO: use timeout?
    end


    # Disconnects from an endpoint.
    #
    # @param endpoint [String]
    # @return [void]
    #
    def disconnect(endpoint)
      Reactor.run { @engine.disconnect(endpoint) } # TODO: use timeout?
    end


    # Unbinds from an endpoint.
    #
    # @param endpoint [String]
    # @return [void]
    #
    def unbind(endpoint)
      Reactor.run { @engine.unbind(endpoint) } # TODO: use timeout?
    end


    # @return [Async::Promise] resolves when first peer completes handshake
    def peer_connected
      @engine.peer_connected
    end


    # @return [Async::Promise] resolves when first subscriber joins (PUB/XPUB only)
    def subscriber_joined
      @engine.subscriber_joined
    end


    # @return [Async::Promise] resolves when all peers disconnect (after having had peers)
    def all_peers_gone
      @engine.all_peers_gone
    end


    # @return [Integer] current number of peer connections
    def connection_count
      @engine.connections.size
    end


    # Signals end-of-stream on the receive side. A subsequent
    # +#receive+ call that would otherwise block returns +nil+.
    #
    # @return [void]
    #
    def close_read
      @engine.dequeue_recv_sentinel
    end


    # Yields lifecycle events for this socket.
    #
    # Spawns a background fiber that reads from an internal event queue.
    # The block receives {MonitorEvent} instances until the socket is
    # closed or the returned task is stopped.
    #
    # @yield [event] called for each lifecycle event
    # @yieldparam event [MonitorEvent]
    # @return [Async::Task] the monitor task (call +#stop+ to end early)
    #
    # @example
    #   task = socket.monitor do |event|
    #     case event
    #     in type: :connected, endpoint:
    #       puts "peer up: #{endpoint}"
    #     in type: :disconnected, endpoint:
    #       puts "peer down: #{endpoint}"
    #     end
    #   end
    #   # later:
    #   task.stop
    #
    def monitor(verbose: false, &block)
      ensure_parent_task

      queue                   = Async::LimitedQueue.new(64)
      @engine.monitor_queue   = queue
      @engine.verbose_monitor = verbose

      Reactor.run do
        @engine.parent_task.async(transient: true, annotation: "monitor") do
          while (event = queue.dequeue)
            block.call(event)
          end
        rescue Async::Stop
        ensure
          @engine.monitor_queue = nil
          block.call(MonitorEvent.new(type: :monitor_stopped))
        end
      end
    end


    # Disable auto-reconnect for connected endpoints.
    #
    # @param val [Boolean]
    # @return [void]
    #
    def reconnect_enabled=(val)
      @engine.reconnect_enabled = val
    end


    # Closes the socket and releases all resources. Drains pending sends
    # up to +linger+ seconds, then cascades teardown through the
    # socket-level Async::Barrier — every connection's per-connection
    # barrier is stopped, cancelling every pump.
    #
    # @return [nil]
    #
    def close
      Reactor.run { @engine.close } # TODO: use timeout?
      nil
    end


    # Immediate hard stop. Skips the linger drain and cascades teardown
    # through the socket-level Async::Barrier. Intended for crash-path
    # cleanup or when the caller already knows no pending sends matter.
    #
    # @return [nil]
    #
    def stop
      Reactor.run { @engine.stop } # TODO: use timeout?
      nil
    end


    # Set socket to use unbounded pipes (HWM=0).
    #
    # @return [nil]
    #
    def set_unbounded
      @options.send_hwm = 0
      @options.recv_hwm = 0
      nil
    end


    # @return [String]
    #
    def inspect
      format("#<%s bound=%p>", self.class, @engine.listeners.keys)
    end


    # Connects or binds based on endpoint prefix convention. Called
    # from subclass initializers (including out-of-tree socket types).
    #
    # @param endpoints [String, nil]
    # @param default [Symbol] :connect or :bind
    #
    def attach_endpoints(endpoints, default:)
      return unless endpoints

      if endpoints.is_a?(Array)
        endpoints.each { |ep| attach_endpoints(ep, default: default) }
        return
      end

      case endpoints
      when /\A@(.+)\z/
        bind($1)
      when /\A>(.+)\z/
        connect($1)
      else
        __send__(default, endpoints)
      end
    end


    # Initializes engine and options for a socket type. Called from
    # subclass initializers (including out-of-tree socket types).
    #
    # @param socket_type [Symbol]
    #
    def init_engine(socket_type, send_hwm: nil, recv_hwm: nil,
                    send_timeout: nil, recv_timeout: nil, conflate: false,
                    on_mute: nil, backend: nil)
      @options = Options.new
      @options.send_hwm     = send_hwm     if send_hwm
      @options.recv_hwm     = recv_hwm     if recv_hwm
      @options.send_timeout = send_timeout if send_timeout
      @options.recv_timeout = recv_timeout if recv_timeout
      @options.conflate     = conflate
      @options.on_mute      = on_mute      if on_mute
      backend_name = (backend || :ruby).to_sym
      require_backend(backend_name) unless Backend.registered?(backend_name)

      engine_class = Backend.fetch(backend_name)
      raise ArgumentError, "unknown backend: #{backend}" unless engine_class

      @engine = engine_class.new(socket_type, @options)
    end


    private


    def require_backend(name)
      path = case name
             when :libzmq then "omq/backend/libzmq"
             when :ffi then "omq/ffi"
             when :rust then "omq/rust"
             end
      return unless path

      require path
    end


    # Sets the engine's parent task before the first bind or connect.
    # Must be called OUTSIDE Reactor.run so that non-Async callers
    # get the IO thread's root task, not an ephemeral work task.
    #
    def ensure_parent_task(parent: nil)
      @engine.capture_parent_task(parent: parent)
    end

  end
end
