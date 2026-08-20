# frozen_string_literal: true

require "async"
require_relative "fd_watcher"

module OMQ
  module Rust
    class Engine
      attr_reader :options, :connections, :routing, :socket_type
      attr_reader :peer_connected, :all_peers_gone, :parent_task
      attr_reader :on_io_thread
      alias on_io_thread? on_io_thread
      attr_writer :reconnect_enabled
      attr_accessor :subscriber_joined


      def initialize(socket_type, options)
        @socket_type          = socket_type
        @options              = options
        @peer_connected       = Async::Promise.new
        @all_peers_gone       = Async::Promise.new
        @subscriber_joined    = Async::Promise.new
        @connections          = {}
        @closed               = false
        @parent_task          = nil
        @on_io_thread         = false
        @materialized         = false
        @recv_sentinels       = 0
        @compression_options  = {}
        @socket               = nil
        @monitor_task         = nil
        @monitor_watching     = false
        @had_peer             = false

        @routing = RoutingStub.new(self)
      end


      def capture_parent_task(parent: nil)
        return if @parent_task

        if parent
          @parent_task = parent
        elsif Async::Task.current?
          @parent_task = Async::Task.current
        elsif !Reactor.native_fiber_scheduler?
          @parent_task = nil
        else
          @parent_task  = Reactor.root_task
          @on_io_thread = true
          Reactor.track_linger(@options.linger)
        end
      end


      def bind(endpoint, parent: nil, **opts)
        capture_parent_task(parent: parent)
        apply_endpoint_options!(opts)
        ensure_materialized
        resolved = @socket.bind(endpoint)
        URI.parse(resolved)
      end


      def connect(endpoint, parent: nil, **opts)
        capture_parent_task(parent: parent)
        apply_endpoint_options!(opts)
        ensure_materialized
        @socket.connect(endpoint)
        URI.parse(endpoint)
      end


      def disconnect(endpoint)
        @socket.disconnect(endpoint)
      end


      def unbind(endpoint)
        @socket.unbind(endpoint)
      end


      def enqueue_send(parts)
        ensure_materialized
        @socket.send(normalize_outgoing(parts))
        nil
      end


      def dequeue_recv
        ensure_materialized

        msg = try_recv
        return msg if msg

        return take_recv_sentinel if @recv_sentinels.positive?

        loop do
          @socket.wait_readable
          msg = try_recv
          return msg if msg

          return take_recv_sentinel if @recv_sentinels.positive?
        end
      end


      def dequeue_recv_sentinel
        @recv_sentinels += 1
        @socket.wake_recv if @materialized
        nil
      end


      def close
        return if @closed

        @closed = true
        @peer_connected.resolve(nil) unless @peer_connected.resolved?
        @all_peers_gone.resolve(nil) unless @all_peers_gone.resolved?
        @subscriber_joined.resolve(nil) unless @subscriber_joined.resolved?
        FdWatcher.unwatch_owner(self) unless Reactor.native_fiber_scheduler?
        @socket&.close
        @monitor_task&.stop
      end


      def closed?
        @closed
      end


      def subscribe(prefix)
        @routing.subscribe(prefix)
      end


      def unsubscribe(prefix)
        @routing.unsubscribe(prefix)
      end


      def emit_monitor_event(type, endpoint: nil, detail: nil)
      end


      def monitor_queue=(queue)
        @monitor_queue = queue
        return unless queue && @materialized

        start_monitor_forwarder
      end


      def verbose_monitor=(val)
        @verbose_monitor = val
      end


      private


      def ensure_materialized
        return if @materialized

        capture_parent_task unless @parent_task
        @socket = OMQ::Rust.socket(@socket_type, **extract_options.transform_keys(&:to_sym))
        @materialized = true
        @routing.replay_pending(@socket)
        start_monitor_forwarder
      end


      def take_recv_sentinel
        @recv_sentinels -= 1
        nil
      end


      def try_recv
        message = @socket.try_recv
        return unless message
        return message unless @socket_type.to_sym == :SERVER

        routing_id, *parts = message
        [[routing_id].pack("N"), *parts]
      end


      def normalize_outgoing(parts)
        return parts unless @socket_type.to_sym == :SERVER

        routing_id, *body = parts
        unless routing_id.is_a?(String) && routing_id.bytesize == 4
          raise ArgumentError, "SERVER routing ID must be a 4-byte String"
        end
        [routing_id.unpack1("N"), *body]
      end


      def start_monitor_forwarder
        return if @monitor_task || @monitor_watching

        if Reactor.native_fiber_scheduler?
          @monitor_task = @parent_task.async(transient: true, annotation: "rust-monitor") do
            monitor_loop
          end
        else
          @monitor_watching = true
          FdWatcher.watch_loop(@socket.monitor_fd, owner: self) do |io|
            io.read_nonblock(256, exception: false)
            while (data = @socket.try_monitor_event)
              handle_monitor_event(data)
            end
            @socket.monitor_fd unless @closed
          end
        end
      end


      def monitor_loop
        until @closed
          data = @socket.monitor.recv
          break unless data

          handle_monitor_event(data)
        end
      rescue IOError, Errno::EBADF
        raise unless @closed
      end


      def handle_monitor_event(data)
        type          = data.fetch(:event)
        endpoint      = data[:endpoint]
        connection_id = data[:connection_id]

        case type
        when :handshake_succeeded
          @had_peer = true
          @connections[connection_id || Object.new] = true
          @peer_connected.resolve(true) unless @peer_connected.resolved?
        when :disconnected
          connection_id ? @connections.delete(connection_id) : @connections.shift
          if @had_peer && @connections.empty? && !@all_peers_gone.resolved?
            @all_peers_gone.resolve(true)
          end
        when :subscribe_received
          @subscriber_joined.resolve(true) unless @subscriber_joined.resolved?
        end

        return unless @monitor_queue

        detail = data.except(:event, :endpoint)
        @monitor_queue.enqueue(MonitorEvent.new(type:, endpoint:, detail: detail.empty? ? nil : detail))
      end


      def extract_options
        h = {}
        h["send_hwm"]           = @options.send_hwm
        h["recv_hwm"]           = @options.recv_hwm
        h["linger"]             = @options.linger == Float::INFINITY ? Float::INFINITY : @options.linger
        h["identity"]           = @options.identity if @options.identity && !@options.identity.empty?
        h["router_mandatory"]   = @options.router_mandatory
        h["conflate"]           = @options.conflate
        h["heartbeat_interval"] = @options.heartbeat_interval
        h["heartbeat_ttl"]      = @options.heartbeat_ttl
        h["heartbeat_timeout"]  = @options.heartbeat_timeout
        h["max_message_size"]   = @options.max_message_size
        h["sndbuf"]             = @options.sndbuf
        h["rcvbuf"]             = @options.rcvbuf
        h["on_mute"]            = @options.on_mute.to_s
        h.merge!(@compression_options)

        ri = @options.reconnect_interval
        if ri.is_a?(Range)
          h["reconnect_interval_min"] = ri.begin.to_f
          h["reconnect_interval_max"] = ri.end.to_f
        elsif ri
          h["reconnect_interval"] = ri.to_f
        end

        extract_mechanism(h)

        h
      end


      def apply_endpoint_options!(opts)
        compression = extract_endpoint_compression_options(opts)
        return if compression.empty?

        if @materialized
          existing = compression.keys.to_h do |key|
            [key, @compression_options.fetch(key, default_compression_option(key))]
          end
          return if compression == existing

          raise ArgumentError,
            "Rust backend compression options must be set before first bind/connect"
        end

        @compression_options.merge!(compression)
      end


      def extract_endpoint_compression_options(opts)
        out = {}

        if opts.key?(:level)
          validate_zstd_level!(opts[:level])
          out["compression_level"] = opts[:level]
        end
        out["compression_dict"] = opts[:dict].b if opts.key?(:dict) && opts[:dict]

        if opts.key?(:auto_dict)
          auto_dict = opts[:auto_dict]
          if auto_dict && opts[:dict]
            raise ArgumentError, "cannot combine auto_dict: and dict:"
          end

          case auto_dict
          when nil, false
            out["compression_auto_train"] = false
          when true
            out["compression_auto_train"] = true
          when Hash
            if auto_dict.key?(:trigger)
              raise ArgumentError,
                "Rust backend does not support auto_dict: trigger"
            end
            validate_positive!("auto_dict capacity", auto_dict[:capacity]) if auto_dict[:capacity]
            out["compression_auto_train"] = true
            out["compression_dict_capacity"] = auto_dict[:capacity] if auto_dict[:capacity]
          else
            raise TypeError, "auto_dict: must be true, false, or a Hash; got #{auto_dict.class}"
          end
        end

        if opts.key?(:compression_threshold)
          out["compression_threshold"] = opts[:compression_threshold]
        end
        out["max_recv_dict_size"] = opts[:max_recv_dict_size] if opts.key?(:max_recv_dict_size)
        if opts.key?(:compression_offload_threshold)
          out["compression_offload_threshold"] = opts[:compression_offload_threshold] || -1
        end

        out
      end


      def default_compression_option(key)
        key == "compression_auto_train" ? false : nil
      end


      def validate_positive!(label, value)
        return if value.respond_to?(:positive?) && value.positive?

        raise ArgumentError, "#{label} must be positive"
      end


      def validate_zstd_level!(level)
        return if level.is_a?(Integer) && (-8..4).cover?(level)

        raise ArgumentError, "zstd compression level must be -8..4, got #{level.inspect}"
      end


      def extract_mechanism(h)
        mech = @options.mechanism
        case mech
        when Protocol::ZMTP::Mechanism::Null
          h["mechanism_type"] = "null"
        else
          klass = mech.class.name
          if klass&.include?("Curve")
            extract_curve_mechanism(h, mech)
          end
        end
      end


      def extract_curve_mechanism(h, mech)
        h["mechanism_type"] = "curve"
        h["mechanism_server"] = mech.instance_variable_get(:@as_server)

        pub_key = mech.instance_variable_get(:@permanent_public)
        sec_key = mech.instance_variable_get(:@permanent_secret)
        h["mechanism_public_key"] = pub_key.to_s.b if pub_key
        h["mechanism_secret_key"] = sec_key.to_s.b if sec_key

        unless h["mechanism_server"]
          srv_key = mech.instance_variable_get(:@server_public)
          h["mechanism_server_key"] = srv_key.to_s.b if srv_key
        end
      end


      class RoutingStub
        def initialize(engine)
          @engine            = engine
          @pending_subscribe = []
          @pending_join      = []
        end


        def subscriber_joined
          @engine.subscriber_joined
        end


        def subscribe(prefix)
          socket = @engine.instance_variable_get(:@socket)
          if @engine.instance_variable_get(:@materialized)
            socket.subscribe(prefix.b)
          else
            @pending_subscribe << prefix.b
          end
        end


        def unsubscribe(prefix)
          @engine.instance_variable_get(:@socket).unsubscribe(prefix.b)
        end


        def join(group)
          socket = @engine.instance_variable_get(:@socket)
          if @engine.instance_variable_get(:@materialized)
            socket.join(group)
          else
            @pending_join << group
          end
        end


        def leave(group)
          @engine.instance_variable_get(:@socket).leave(group)
        end


        def replay_pending(socket)
          @pending_subscribe.each { |p| socket.subscribe(p) }
          @pending_subscribe.clear
          @pending_join.each { |g| socket.join(g) }
          @pending_join.clear
        end
      end

    end
  end
end
