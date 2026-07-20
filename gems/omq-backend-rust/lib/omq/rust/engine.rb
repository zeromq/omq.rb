# frozen_string_literal: true

require "async"

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
        @socket_type    = socket_type
        @options        = options
        @peer_connected = Async::Promise.new
        @all_peers_gone = Async::Promise.new
        @subscriber_joined = Async::Promise.new
        @connections    = []
        @closed         = false
        @parent_task    = nil
        @on_io_thread   = false
        @materialized   = false

        @native = Native::RustSocket.new(socket_type.to_s)

        @routing = RoutingStub.new(self)
      end


      def capture_parent_task(parent: nil)
        return if @parent_task

        if parent
          @parent_task = parent
        elsif Async::Task.current?
          @parent_task = Async::Task.current
        else
          @parent_task  = Reactor.root_task
          @on_io_thread = true
          Reactor.track_linger(@options.linger)
        end
      end


      def bind(endpoint, parent: nil, **)
        capture_parent_task(parent: parent)
        ensure_materialized
        resolved = @native.bind(endpoint)
        URI.parse(resolved)
      end


      def connect(endpoint, parent: nil, **)
        capture_parent_task(parent: parent)
        ensure_materialized
        @native.connect(endpoint)
        URI.parse(endpoint)
      end


      def disconnect(endpoint)
        @native.disconnect(endpoint)
      end


      def unbind(endpoint)
        @native.unbind(endpoint)
      end


      def enqueue_send(parts)
        ensure_materialized
        result = @native.enqueue_send(parts)
        return if result == :ok

        @send_signal_r ||= IO.for_fd(@native.send_fd, autoclose: false)
        loop do
          result = @native.enqueue_send(parts)
          return if result == :ok

          @send_signal_r.wait_readable
          @send_signal_r.read_nonblock(256, exception: false)
        end
      end


      def dequeue_recv
        ensure_materialized

        if @recv_batch && !@recv_batch.empty?
          return @recv_batch.shift
        end

        batch = @native.try_recv_batch
        if batch
          msg = batch.shift
          @recv_batch = batch unless batch.empty?
          return msg
        end

        loop do
          @recv_signal_r.wait_readable
          @recv_signal_r.read_nonblock(256, exception: false)
          batch = @native.try_recv_batch
          if batch
            msg = batch.shift
            @recv_batch = batch unless batch.empty?
            return msg
          end
        end
      end


      def dequeue_recv_sentinel
        @native.try_recv
      end


      def close
        return if @closed

        @closed = true
        @peer_connected.resolve(nil) unless @peer_connected.resolved?
        @all_peers_gone.resolve(nil) unless @all_peers_gone.resolved?
        @subscriber_joined.resolve(nil) unless @subscriber_joined.resolved?
        @native.close
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

        Native.send(:io_threads=, OMQ::Rust.io_threads)
        @native.set_options(extract_options)
        @native.materialize
        @recv_signal_r = IO.for_fd(@native.recv_fd, autoclose: false)
        @materialized  = true

        @routing.replay_pending(@native)

        spawn_lifecycle_watcher(@native.peer_connected_fd, @peer_connected)
        spawn_lifecycle_watcher(@native.all_peers_gone_fd, @all_peers_gone)
        spawn_lifecycle_watcher(@native.subscriber_joined_fd, @subscriber_joined)

        start_monitor_forwarder if @monitor_queue
      end


      def spawn_lifecycle_watcher(fd, promise)
        io = IO.for_fd(fd, autoclose: false)
        @parent_task.async(transient: true) do
          io.wait_readable
          promise.resolve(true) unless promise.resolved? || @closed
        rescue IOError, Errno::EBADF
        end
      end


      def start_monitor_forwarder
        monitor_io = IO.for_fd(@native.monitor_fd, autoclose: false)
        @parent_task.async(transient: true, annotation: "rust-monitor") do
          loop do
            monitor_io.wait_readable
            monitor_io.read_nonblock(256, exception: false)
            while (data = @native.try_recv_monitor)
              @monitor_queue.enqueue(MonitorEvent.new(**data))
            end
          end
        end
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
          native = @engine.instance_variable_get(:@native)
          if @engine.instance_variable_get(:@materialized)
            native.subscribe(prefix.b)
          else
            @pending_subscribe << prefix.b
          end
        end


        def unsubscribe(prefix)
          @engine.instance_variable_get(:@native).unsubscribe(prefix.b)
        end


        def join(group)
          native = @engine.instance_variable_get(:@native)
          if @engine.instance_variable_get(:@materialized)
            native.join(group)
          else
            @pending_join << group
          end
        end


        def leave(group)
          @engine.instance_variable_get(:@native).leave(group)
        end


        def replay_pending(native)
          @pending_subscribe.each { |p| native.subscribe(p) }
          @pending_subscribe.clear
          @pending_join.each { |g| native.join(g) }
          @pending_join.clear
        end
      end

    end
  end
end
