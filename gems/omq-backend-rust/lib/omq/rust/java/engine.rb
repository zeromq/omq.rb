# frozen_string_literal: true

require "jar_dependencies"
require "thread"
require "uri"
require "java"

require_relative "../version"
require_relative "platform"

require_jar "io.github.paddor", "omq-java",
            OMQ::Rust::Java.classifier, OMQ::Rust::OMQ_JAVA_VERSION

module OMQ
  module Rust
    module Java
      Duration = ::Java::JavaTime::Duration
      JavaOMQ = ::Java::IoOmq::OMQ
      SocketOptions = ::Java::IoOmq::SocketOptions
      SocketType = ::Java::IoOmq::SocketType
      Message = ::Java::IoOmq::Message
      CurveKeypair = ::Java::IoOmq::CurveKeypair
      OnMute = ::Java::IoOmq::OnMute
      @context_mutex = Mutex.new

      class << self
        def context
          @context_mutex.synchronize do
            @context ||= JavaOMQ.context(OMQ::Rust.io_threads.to_i).tap do
              at_exit { @context&.close rescue nil } unless @context_at_exit
              @context_at_exit = true
            end
          end
        end
      end

      class Promise
        def initialize(&resolver)
          @mutex    = Mutex.new
          @cv       = ConditionVariable.new
          @resolved = false
          @value    = nil
          @resolver = resolver
        end


        def resolved?
          @mutex.synchronize { @resolved }
        end


        def resolve(value = nil)
          @mutex.synchronize do
            return @value if @resolved

            @resolved = true
            @value    = value
            @cv.broadcast
          end
        end


        def wait
          until resolved?
            if @resolver
              @resolver.call(self)
            else
              @mutex.synchronize { @cv.wait(@mutex, 0.05) unless @resolved }
            end
          end

          @value
        end
      end

      class Engine
        POLL_SECONDS = 0.005
        POLL_DURATION = Duration.ofMillis((POLL_SECONDS * 1000).to_i)

        attr_reader :options, :connections, :routing, :socket_type
        attr_reader :peer_connected, :all_peers_gone, :parent_task
        attr_reader :on_io_thread
        alias on_io_thread? on_io_thread
        attr_writer :reconnect_enabled
        attr_accessor :subscriber_joined


        def initialize(socket_type, options)
          @socket_type         = socket_type
          @options             = options
          @connections         = {}
          @closed              = false
          @parent_task         = nil
          @on_io_thread        = false
          @materialized        = false
          @recv_sentinels      = 0
          @compression_options = {}

          @peer_connected = Promise.new do |promise|
            wait_connected(1)
            promise.resolve(true) unless @closed
          end
          @all_peers_gone = Promise.new do |promise|
            wait_all_peers_gone
            promise.resolve(true) unless @closed
          end
          @subscriber_joined = Promise.new do |promise|
            wait_subscribed(1)
            promise.resolve(true) unless @closed
          end

          @routing = RoutingStub.new(self)
        end


        def capture_parent_task(parent: nil)
          @parent_task ||= parent
        end


        def bind(endpoint, parent: nil, **opts)
          capture_parent_task(parent: parent)
          apply_endpoint_options!(opts)
          ensure_materialized
          URI.parse(with_java_errors { @native.bind(endpoint) })
        end


        def connect(endpoint, parent: nil, **opts)
          capture_parent_task(parent: parent)
          apply_endpoint_options!(opts)
          ensure_materialized
          with_java_errors { @native.connect(endpoint) }
          resolve_peer_connected if endpoint.start_with?("inproc://")
          URI.parse(endpoint)
        end


        def disconnect(endpoint)
          ensure_materialized
          with_java_errors { @native.disconnect(endpoint) }
        end


        def unbind(endpoint)
          ensure_materialized
          with_java_errors { @native.unbind(endpoint) }
        end


        def enqueue_send(parts)
          ensure_materialized
          msg = java_message(parts)

          if (timeout = @options.write_timeout)
            ok = with_java_errors { @native.send(msg, duration_from_seconds("write_timeout", timeout)) }
            raise IO::TimeoutError, "operation timed out" unless ok
          else
            with_java_errors { @native.send(msg) }
          end

          nil
        end


        def dequeue_recv
          ensure_materialized
          @recv_deadline = monotonic_time + @options.read_timeout.to_f if @options.read_timeout

          loop do
            return take_recv_sentinel if @recv_sentinels.positive?

            optional = with_java_errors { @native.tryReceive }
            return ruby_parts(optional.get) if optional.isPresent

            raise IO::TimeoutError, "operation timed out" if recv_deadline_expired?

            sleep recv_poll_seconds
          end
        ensure
          @recv_deadline = nil
        end


        def dequeue_recv_sentinel
          @recv_sentinels += 1
          nil
        end


        def close
          return if @closed

          @closed = true
          @peer_connected.resolve(nil)
          @all_peers_gone.resolve(nil)
          @subscriber_joined.resolve(nil)
          with_java_errors { @native&.close }
          @connections.clear
          nil
        end


        alias stop close


        def closed?
          @closed
        end


        def subscribe(prefix)
          @routing.subscribe(prefix)
        end


        def unsubscribe(prefix)
          @routing.unsubscribe(prefix)
        end


        def emit_monitor_event(_type, endpoint: nil, detail: nil)
        end


        def monitor_queue=(queue)
          @monitor_queue = queue
        end


        def verbose_monitor=(val)
          @verbose_monitor = val
        end


        private


        def ensure_materialized
          return if @materialized

          @native = Java.context.socket(SocketType.valueOf(@socket_type.to_s), java_options)
          @materialized = true
          @routing.replay_pending(@native)
        end


        def java_message(parts)
          frames = parts.map { |part| part.to_java_bytes }
          return Message.of(frames.first) if frames.size == 1

          Message.multipart(*frames)
        end


        def ruby_parts(message)
          parts = []
          message.partCount.times do |index|
            parts << String.from_java_bytes(message.part(index)).b.freeze
          end
          parts.freeze
        end


        def take_recv_sentinel
          @recv_sentinels -= 1
          nil
        end


        def recv_deadline_expired?
          return false unless @options.read_timeout

          monotonic_time >= @recv_deadline
        end


        def recv_poll_seconds
          return POLL_SECONDS unless @options.read_timeout

          [[@recv_deadline - monotonic_time, POLL_SECONDS].min, 0].max
        end


        def monotonic_time
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end


        def wait_connected(count)
          ensure_materialized

          loop do
            result = @native.waitConnected(count, POLL_DURATION)
            resolve_peer_connected if result.to_i.positive?
            return result
          rescue ::Java::IoOmq::TimeoutException
            return nil if @closed
          rescue ::Java::IoOmq::ClosedException
            return nil
          end
        end


        def resolve_peer_connected
          @connections[:peer] = true
          @peer_connected.resolve(true) unless @peer_connected.resolved?
        end


        def wait_subscribed(count)
          ensure_materialized

          loop do
            result = @native.waitSubscribed(count, POLL_DURATION)
            return result
          rescue ::Java::IoOmq::TimeoutException
            return nil if @closed
          rescue ::Java::IoOmq::ClosedException
            return nil
          end
        end


        def wait_all_peers_gone
          @peer_connected.wait

          loop do
            @native.waitConnected(1, POLL_DURATION)
            sleep POLL_SECONDS
          rescue ::Java::IoOmq::TimeoutException
            @connections.clear
            return true
          rescue ::Java::IoOmq::ClosedException
            return nil
          end
        end


        def java_options
          builder = SocketOptions.builder
          apply_socket_options(builder)
          apply_compression_options(builder)
          apply_mechanism(builder)
          builder.build
        end


        def apply_socket_options(builder)
          builder.sendHighWaterMark([integer_option("send_hwm", @options.send_hwm), 0].max)
          builder.receiveHighWaterMark([integer_option("recv_hwm", @options.recv_hwm), 0].max)

          if @options.linger == Float::INFINITY
            builder.lingerForever
          else
            builder.linger(duration_from_seconds("linger", @options.linger))
          end

          builder.identity(bytes(@options.identity)) if @options.identity && !@options.identity.empty?
          builder.routerMandatory(!!@options.router_mandatory)
          builder.conflate(!!@options.conflate)
          builder.maxMessageSize(nonnegative_integer("max_message_size", @options.max_message_size)) if @options.max_message_size
          builder.sendBufferSize(nonnegative_integer("sndbuf", @options.sndbuf)) if @options.sndbuf
          builder.receiveBufferSize(nonnegative_integer("rcvbuf", @options.rcvbuf)) if @options.rcvbuf
          builder.onMute(on_mute)

          apply_duration_option(builder, :heartbeatInterval, "heartbeat_interval", @options.heartbeat_interval)
          apply_duration_option(builder, :heartbeatTtl, "heartbeat_ttl", @options.heartbeat_ttl)
          apply_duration_option(builder, :heartbeatTimeout, "heartbeat_timeout", @options.heartbeat_timeout)
          apply_reconnect_option(builder)
        end


        def apply_reconnect_option(builder)
          reconnect = @options.reconnect_interval

          case reconnect
          when Range
            builder.reconnectExponential(
              duration_from_seconds("reconnect_interval min", reconnect.begin),
              duration_from_seconds("reconnect_interval max", reconnect.end),
            )
          when nil, false
            builder.reconnectDisabled
          else
            builder.reconnectInterval(duration_from_seconds("reconnect_interval", reconnect))
          end
        end


        def apply_duration_option(builder, method, label, value)
          builder.public_send(method, duration_from_seconds(label, value)) if value
        end


        def apply_compression_options(builder)
          if @compression_options.key?("compression_auto_train")
            builder.compressionAutoTrain(!!@compression_options["compression_auto_train"])
          end
          if (value = @compression_options["compression_threshold"])
            builder.compressionThreshold(nonnegative_integer("compression_threshold", value))
          end
          if (value = @compression_options["compression_level"])
            builder.compressionLevel(integer_option("compression_level", value))
          end
          if (value = @compression_options["compression_dict"])
            builder.compressionDict(bytes(value))
          end
          if (value = @compression_options["compression_dict_capacity"])
            builder.compressionDictCapacity(nonnegative_integer("compression_dict_capacity", value))
          end
          if (value = @compression_options["max_recv_dict_size"])
            builder.maxReceiveDictSize(nonnegative_integer("max_recv_dict_size", value))
          end

          return unless @compression_options.key?("compression_offload_threshold")

          value = @compression_options["compression_offload_threshold"]
          if value.nil? || value.to_i.negative?
            builder.noCompressionOffload
          else
            builder.compressionOffloadThreshold(nonnegative_integer("compression_offload_threshold", value))
          end
        end


        def apply_mechanism(builder)
          mech = @options.mechanism
          klass = mech.class.name
          return unless klass&.include?("Curve")

          require "protocol/zmtp/z85"

          keypair = CurveKeypair.new(
            z85_key(mech.instance_variable_get(:@permanent_public), "public key"),
            z85_key(mech.instance_variable_get(:@permanent_secret), "secret key"),
          )

          if mech.instance_variable_get(:@as_server)
            builder.curveServer(keypair)
          else
            builder.curveClient(keypair, z85_key(mech.instance_variable_get(:@server_public), "server key"))
          end
        end


        def z85_key(key, label)
          raw = key&.to_s&.b
          raise ArgumentError, "#{label} must be exactly 32 bytes" unless raw&.bytesize == 32

          Protocol::ZMTP::Z85.encode(raw)
        end


        def on_mute
          case @options.on_mute&.to_sym
          when :drop_newest, :drop
            OnMute::DROP_NEWEST
          when :drop_oldest
            OnMute::DROP_OLDEST
          else
            OnMute::BLOCK
          end
        end


        def duration_from_seconds(label, value)
          seconds = Float(value)
          raise ArgumentError, "#{label} must be finite and non-negative" unless seconds.finite? && !seconds.negative?

          Duration.ofNanos((seconds * 1_000_000_000).round)
        end


        def nonnegative_integer(label, value)
          value = integer_option(label, value)
          raise ArgumentError, "#{label} must be non-negative" if value.negative?

          value
        end


        def integer_option(label, value)
          Integer(value)
        rescue ArgumentError, TypeError
          raise ArgumentError, "#{label} must be an Integer"
        end


        def bytes(value)
          value.to_s.b.to_java_bytes
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

          out["compression_threshold"] = opts[:compression_threshold] if opts.key?(:compression_threshold)
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


        def with_java_errors
          yield
        rescue ::Java::IoOmq::TimeoutException => error
          raise IO::TimeoutError, error.message
        rescue ::Java::IoOmq::ClosedException => error
          raise IOError, error.message
        rescue ::Java::IoOmq::InvalidEndpointException => error
          raise ArgumentError, error.message
        rescue ::Java::IoOmq::OMQException => error
          raise RuntimeError, error.message
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
              native.subscribe(prefix.b.to_java_bytes)
            else
              @pending_subscribe << prefix.b
            end
          end


          def unsubscribe(prefix)
            @engine.instance_variable_get(:@native).unsubscribe(prefix.b.to_java_bytes)
          end


          def join(group)
            native = @engine.instance_variable_get(:@native)
            if @engine.instance_variable_get(:@materialized)
              native.join(group.b.to_java_bytes)
            else
              @pending_join << group.b
            end
          end


          def leave(group)
            @engine.instance_variable_get(:@native).leave(group.b.to_java_bytes)
          end


          def replay_pending(native)
            @pending_subscribe.each { |prefix| native.subscribe(prefix.to_java_bytes) }
            @pending_subscribe.clear
            @pending_join.each { |group| native.join(group.to_java_bytes) }
            @pending_join.clear
          end
        end
      end
    end

    Engine = Java::Engine
  end
end
