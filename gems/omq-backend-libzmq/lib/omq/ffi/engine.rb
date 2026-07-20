# frozen_string_literal: true

require "async"
require "uri"

module OMQ
  module FFI
    # FFI Engine — wraps a libzmq socket to implement the OMQ Engine contract.
    #
    # A dedicated I/O thread owns the zmq_socket exclusively (libzmq sockets
    # are not thread-safe). Send and recv flow through queues, with an IO pipe
    # to wake the Async fiber scheduler.
    #
    class Engine
      L = Libzmq


      # @return [Options] socket options
      attr_reader :options
      # @return [Array] active connections
      attr_reader :connections
      # @return [RoutingStub] subscription/group routing interface
      attr_reader :routing
      # @return [Async::Promise] resolved when the first peer connects
      attr_reader :peer_connected
      # @return [Async::Promise] resolved when all peers have disconnected
      attr_reader :all_peers_gone
      # @return [Async::Task, nil] root of the engine's task tree
      attr_reader :parent_task
      # @return [Boolean] true when the engine's parent task lives on the
      #   shared {OMQ::Reactor} IO thread (i.e. not created under an
      #   Async task). Writable/Readable check this to pick the fast path.
      attr_reader :on_io_thread
      alias on_io_thread? on_io_thread
      # @param value [Boolean] enables or disables automatic reconnection
      attr_writer :reconnect_enabled
      # @note Monitor events are not yet emitted by the FFI backend; these
      #   writers exist so Socket#monitor can attach without raising. Wiring
      #   libzmq's zmq_socket_monitor is a TODO.
      attr_writer :monitor_queue, :verbose_monitor

      # Routing stub that delegates subscribe/unsubscribe/join/leave to
      # libzmq socket options via the I/O thread.
      #
      class RoutingStub
        # @return [Async::Promise] resolved when a subscriber joins
        attr_reader :subscriber_joined

        # @param engine [Engine] the parent engine instance
        def initialize(engine)
          @engine            = engine
          @subscriber_joined = Async::Promise.new
        end


        # Subscribes to messages matching the given prefix.
        #
        # @param prefix [String] subscription prefix
        # @return [void]
        def subscribe(prefix)
          @engine.send_cmd(:subscribe, prefix.b)
        end


        # Removes a subscription for the given prefix.
        #
        # @param prefix [String] subscription prefix to remove
        # @return [void]
        def unsubscribe(prefix)
          @engine.send_cmd(:unsubscribe, prefix.b)
        end


        # Joins a DISH group for receiving RADIO messages.
        #
        # @param group [String] group name
        # @return [void]
        def join(group)
          @engine.send_cmd(:join, group)
        end


        # Leaves a DISH group.
        #
        # @param group [String] group name
        # @return [void]
        def leave(group)
          @engine.send_cmd(:leave, group)
        end
      end


      # Maps an OMQ +linger+ value (seconds, or +nil+/+Float::INFINITY+
      # for "wait forever") to libzmq's ZMQ_LINGER int milliseconds
      # (-1 = infinite, 0 = drop, N = N ms).
      #
      # @param linger [Numeric, nil]
      # @return [Integer]
      #
      def self.linger_to_zmq_ms(linger)
        return -1 if linger.nil? || linger == Float::INFINITY
        (linger * 1000).to_i
      end


      # @param socket_type [Symbol] e.g. :REQ, :PAIR
      # @param options [Options]
      #
      def initialize(socket_type, options)
        @socket_type    = socket_type
        @options        = options
        @peer_connected = Async::Promise.new
        @all_peers_gone = Async::Promise.new
        @connections    = []
        @closed         = false
        @parent_task    = nil
        @on_io_thread   = false

        @zmq_socket = L.zmq_socket(OMQ::FFI.context, L::SOCKET_TYPES.fetch(@socket_type))
        raise "zmq_socket failed: #{L.zmq_strerror(L.zmq_errno)}" if @zmq_socket.null?

        apply_options

        @routing = RoutingStub.new(self)

        # Queues for cross-thread communication
        @send_queue = Thread::Queue.new   # main → io thread
        @recv_queue = Thread::Queue.new   # io thread → main
        @cmd_queue  = Thread::Queue.new   # control commands → io thread

        # Signal pipe: io thread → Async fiber (message received)
        @recv_signal_r, @recv_signal_w = IO.pipe
        # Wake pipe: main thread → io thread (send/cmd enqueued)
        @wake_r, @wake_w = IO.pipe

        @io_thread = nil
      end


      # --- Socket lifecycle ---

      # Binds the socket to the given endpoint.
      #
      # @param endpoint [String] ZMQ endpoint URL (e.g. "tcp://*:5555")
      # @return [URI::Generic] resolved endpoint URI (with auto-selected port for "tcp://host:0")
      def bind(endpoint)
        sync_identity
        send_cmd(:bind, endpoint)
        resolved = get_string_option(L::ZMQ_LAST_ENDPOINT)
        @connections << :libzmq
        @peer_connected.resolve(:libzmq) unless @peer_connected.resolved?
        URI.parse(resolved)
      end


      # Connects the socket to the given endpoint.
      #
      # @param endpoint [String] ZMQ endpoint URL
      # @return [URI::Generic] parsed endpoint URI
      def connect(endpoint)
        sync_identity
        send_cmd(:connect, endpoint)
        @connections << :libzmq
        @peer_connected.resolve(:libzmq) unless @peer_connected.resolved?
        URI.parse(endpoint)
      end


      # Disconnects from the given endpoint.
      #
      # @param endpoint [String] ZMQ endpoint URL
      # @return [void]
      def disconnect(endpoint)
        send_cmd(:disconnect, endpoint)
      end


      # Unbinds from the given endpoint.
      #
      # @param endpoint [String] ZMQ endpoint URL
      # @return [void]
      def unbind(endpoint)
        send_cmd(:unbind, endpoint)
      end


      # Subscribes to a topic prefix (SUB/XSUB). Delegates to the routing
      # stub for API parity with the pure-Ruby Engine.
      #
      # @param prefix [String]
      # @return [void]
      def subscribe(prefix)
        @routing.subscribe(prefix)
      end


      # Unsubscribes from a topic prefix (SUB/XSUB).
      #
      # @param prefix [String]
      # @return [void]
      def unsubscribe(prefix)
        @routing.unsubscribe(prefix)
      end


      # @return [Async::Promise] resolved when a subscriber joins (PUB/XPUB).
      def subscriber_joined
        @routing.subscriber_joined
      end


      # Closes the socket and shuts down the I/O thread.
      #
      # Honors `options.linger`:
      #   nil → wait forever for Ruby-side queue to drain into libzmq
      #         and for libzmq's own LINGER to flush to the network
      #   0   → drop anything not yet in libzmq's kernel buffers, close fast
      #   N   → up to N seconds for drain + N + 1s grace for join
      #
      # @return [void]
      def close
        return if @closed
        @closed = true
        if @io_thread
          @cmd_queue.push([:stop])
          wake_io_thread
          linger = @options.linger
          if linger.nil?
            @io_thread.join
          elsif linger.zero?
            @io_thread.join(0.5) # fast path: zmq_close is non-blocking with LINGER=0
          else
            @io_thread.join(linger + 1.0)
          end
          @io_thread.kill if @io_thread.alive? # hard stop if deadline exceeded
        else
          # IO thread never started — close socket directly
          L.zmq_close(@zmq_socket)
        end
        @recv_signal_r&.close rescue nil
        @recv_signal_w&.close rescue nil
        @wake_r&.close rescue nil
        @wake_w&.close rescue nil
      end


      # Captures the current Async task as the parent for I/O scheduling.
      # +parent:+ is accepted for API compatibility with the pure-Ruby
      # engine but has no effect: the FFI backend runs its own I/O
      # thread and doesn't participate in the Async barrier tree.
      #
      # @return [void]
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


      # --- Send ---

      # Enqueues a multipart message for sending via the I/O thread.
      #
      # @param parts [Array<String>] message frames
      # @return [void]
      def enqueue_send(parts)
        ensure_io_thread
        @send_queue.push(parts)
        wake_io_thread
      end


      # --- Recv ---

      # Dequeues the next received message, blocking until one is available.
      #
      # @return [Array<String>] multipart message
      def dequeue_recv
        ensure_io_thread
        wait_for_message
      end


      # Pushes a nil sentinel into the recv queue to unblock a waiting consumer.
      #
      # @return [void]
      def dequeue_recv_sentinel
        @recv_queue.push(nil)
        @recv_signal_w.write_nonblock(".", exception: false) rescue nil
      end


      # Send a control command to the I/O thread.
      # @api private
      #
      def send_cmd(cmd, *args)
        ensure_io_thread
        result = Thread::Queue.new
        @cmd_queue.push([cmd, args, result])
        wake_io_thread
        r = result.pop
        raise r if r.is_a?(Exception)
        r
      end


      # Wakes the I/O thread via the internal pipe.
      #
      # @return [void]
      def wake_io_thread
        @wake_w.write_nonblock(".", exception: false)
      end

      private

      # Waits for a message from the I/O thread's recv queue.
      # Uses the signal pipe so Async can yield the fiber.
      #
      def wait_for_message
        loop do
          begin
            return @recv_queue.pop(true)
          rescue ThreadError
            # empty
          end
          @recv_signal_r.wait_readable
          @recv_signal_r.read_nonblock(256, exception: false)
        end
      end


      def ensure_io_thread
        return if @io_thread
        @io_thread = Thread.new { io_loop }
      end


      # The I/O loop runs on a dedicated thread. It owns the zmq_socket
      # exclusively and processes commands, sends, and recvs.
      #
      def io_loop
        zmq_fd_io = IO.for_fd(get_zmq_fd, autoclose: false)

        loop do
          drain_cmds or break
          drain_sends
          try_recv

          # Block until ZMQ or wake pipe has activity.
          IO.select([zmq_fd_io, @wake_r], nil, nil, 0.1)
          @wake_r.read_nonblock(4096, exception: false)
        end
      rescue
        # Thread exit
      ensure
        # Drain Ruby-side send queue into libzmq, bounded by linger deadline.
        # Then re-apply current linger to libzmq (user may have changed it
        # after apply_options ran in initialize) and zmq_close uses it to
        # flush libzmq's own queue to TCP.
        drain_sends_with_deadline(zmq_fd_io, shutdown_deadline) rescue nil
        set_int_option(L::ZMQ_LINGER, Engine.linger_to_zmq_ms(@options.linger)) rescue nil
        zmq_fd_io&.close rescue nil
        L.zmq_close(@zmq_socket)
      end


      # Returns a monotonic deadline for the Ruby-side drain phase, or nil
      # for infinite, or the current clock for "drop immediately".
      #
      def shutdown_deadline
        linger = @options.linger
        return nil if linger.nil?
        now = Async::Clock.now
        return now if linger.zero?
        now + linger
      end


      # Retries drain_sends with IO.select until either the Ruby-side queue
      # is empty or the deadline is hit. nil deadline = wait forever.
      #
      def drain_sends_with_deadline(zmq_fd_io, deadline)
        loop do
          drain_sends
          break if @pending_send.nil? && @send_queue.empty?
          if deadline
            remaining = deadline - Async::Clock.now
            break if remaining <= 0
            IO.select([zmq_fd_io], nil, nil, [remaining, 0.1].min)
          else
            IO.select([zmq_fd_io], nil, nil, 0.1)
          end
        end
      end


      def zmq_has_events?
        @events_buf ||= ::FFI::MemoryPointer.new(:int)
        @events_len ||= ::FFI::MemoryPointer.new(:size_t).tap { |p| p.write(:size_t, ::FFI.type_size(:int)) }
        L.zmq_getsockopt(@zmq_socket, L::ZMQ_EVENTS, @events_buf, @events_len)
        @events_buf.read_int != 0
      end


      def drain_cmds
        loop do
          begin
            cmd = @cmd_queue.pop(true)
          rescue ThreadError
            return true  # queue empty, continue
          end
          return false unless process_cmd(cmd)
        end
      end


      def process_cmd(cmd)
        name, args, result = cmd
        case name
        when :stop
          result&.push(nil)
          return false
        when :bind
          rc = L.zmq_bind(@zmq_socket, args[0])
          result&.push(rc >= 0 ? nil : syscall_error)
        when :connect
          rc = L.zmq_connect(@zmq_socket, args[0])
          result&.push(rc >= 0 ? nil : syscall_error)
        when :disconnect
          rc = L.zmq_disconnect(@zmq_socket, args[0])
          result&.push(rc >= 0 ? nil : syscall_error)
        when :unbind
          rc = L.zmq_unbind(@zmq_socket, args[0])
          result&.push(rc >= 0 ? nil : syscall_error)
        when :set_identity
          set_bytes_option(L::ZMQ_IDENTITY, args[0])
          result&.push(nil)
        when :subscribe
          set_bytes_option(L::ZMQ_SUBSCRIBE, args[0])
          result&.push(nil)
        when :unsubscribe
          set_bytes_option(L::ZMQ_UNSUBSCRIBE, args[0])
          result&.push(nil)
        when :join
          rc = L.respond_to?(:zmq_join) ? L.zmq_join(@zmq_socket, args[0]) : -1
          result&.push(rc >= 0 ? nil : RuntimeError.new("zmq_join not available"))
        when :leave
          rc = L.respond_to?(:zmq_leave) ? L.zmq_leave(@zmq_socket, args[0]) : -1
          result&.push(rc >= 0 ? nil : RuntimeError.new("zmq_leave not available"))
        when :drain_send
          # handled in drain_sends
          result&.push(nil)
        end
        true
      end


      def try_recv
        loop do
          parts = recv_multipart_nonblock
          break unless parts
          @recv_queue.push(parts.freeze)
          @recv_signal_w.write_nonblock(".", exception: false)
        end
      end


      def drain_sends
        @pending_send ||= nil
        loop do
          parts = @pending_send || begin
            @send_queue.pop(true)
          rescue ThreadError
            break
          end
          if send_multipart_nonblock(parts)
            @pending_send = nil
          else
            @pending_send = parts  # retry next cycle (HWM reached)
            break
          end
        end
      end


      # Returns true if fully sent, false if would block (HWM).
      #
      def send_multipart_nonblock(parts)
        parts.each_with_index do |part, i|
          flags = L::ZMQ_DONTWAIT
          flags |= L::ZMQ_SNDMORE if i < parts.size - 1
          msg = L.alloc_msg
          L.zmq_msg_init_size(msg, part.bytesize)
          L.zmq_msg_data(msg).write_bytes(part)
          rc = L.zmq_msg_send(msg, @zmq_socket, flags)
          if rc < 0
            L.zmq_msg_close(msg)
            return false  # EAGAIN — would block
          end
        end
        true
      end


      def recv_multipart_nonblock
        parts = []
        loop do
          msg = L.alloc_msg
          L.zmq_msg_init(msg)
          rc = L.zmq_msg_recv(msg, @zmq_socket, L::ZMQ_DONTWAIT)
          if rc < 0
            L.zmq_msg_close(msg)
            return parts.empty? ? nil : parts  # EAGAIN = no more data
          end

          size = L.zmq_msg_size(msg)
          data = L.zmq_msg_data(msg).read_bytes(size)
          L.zmq_msg_close(msg)
          parts << data.freeze

          break unless rcvmore?
        end
        parts.empty? ? nil : parts
      end


      def rcvmore?
        buf = ::FFI::MemoryPointer.new(:int)
        len = ::FFI::MemoryPointer.new(:size_t)
        len.write(:size_t, ::FFI.type_size(:int))
        L.zmq_getsockopt(@zmq_socket, L::ZMQ_RCVMORE, buf, len)
        buf.read_int != 0
      end


      def get_zmq_fd
        buf = ::FFI::MemoryPointer.new(:int)
        len = ::FFI::MemoryPointer.new(:size_t)
        len.write(:size_t, ::FFI.type_size(:int))
        L.zmq_getsockopt(@zmq_socket, L::ZMQ_FD, buf, len)
        buf.read_int
      end


      # Re-syncs identity to libzmq (user may set it after construction).
      #
      def sync_identity
        id = @options.identity
        if id && !id.empty?
          send_cmd(:set_identity, id)
        end
      end


      def apply_options
        set_int_option(L::ZMQ_SNDHWM, @options.send_hwm)
        set_int_option(L::ZMQ_RCVHWM, @options.recv_hwm)
        set_int_option(L::ZMQ_LINGER, Engine.linger_to_zmq_ms(@options.linger))
        set_int_option(L::ZMQ_CONFLATE, @options.conflate ? 1 : 0)

        if @options.identity && !@options.identity.empty?
          set_bytes_option(L::ZMQ_IDENTITY, @options.identity)
        end

        if @options.max_message_size
          set_int64_option(L::ZMQ_MAXMSGSIZE, @options.max_message_size)
        end

        if @options.reconnect_interval
          ivl = @options.reconnect_interval
          if ivl.is_a?(Range)
            set_int_option(L::ZMQ_RECONNECT_IVL, (ivl.begin * 1000).to_i)
            set_int_option(L::ZMQ_RECONNECT_IVL_MAX, (ivl.end * 1000).to_i)
          else
            set_int_option(L::ZMQ_RECONNECT_IVL, (ivl * 1000).to_i)
          end
        end

        set_int_option(L::ZMQ_ROUTER_MANDATORY, 1) if @options.router_mandatory
      end


      def set_int_option(opt, value)
        buf = ::FFI::MemoryPointer.new(:int)
        buf.write_int(value)
        L.zmq_setsockopt(@zmq_socket, opt, buf, ::FFI.type_size(:int))
      end


      def set_int64_option(opt, value)
        buf = ::FFI::MemoryPointer.new(:int64)
        buf.write_int64(value)
        L.zmq_setsockopt(@zmq_socket, opt, buf, ::FFI.type_size(:int64))
      end


      def set_bytes_option(opt, value)
        buf = ::FFI::MemoryPointer.from_string(value)
        L.zmq_setsockopt(@zmq_socket, opt, buf, value.bytesize)
      end


      def get_string_option(opt)
        buf = ::FFI::MemoryPointer.new(:char, 256)
        len = ::FFI::MemoryPointer.new(:size_t)
        len.write(:size_t, 256)
        L.check!(L.zmq_getsockopt(@zmq_socket, opt, buf, len), "zmq_getsockopt")
        buf.read_string(len.read(:size_t) - 1)
      end


      # Builds an Errno::XXX exception from the current zmq_errno so callers
      # can rescue the same classes they would from the pure-Ruby backend
      # (e.g. `Errno::EADDRINUSE`, `Errno::ECONNREFUSED`). Falls back to a
      # plain SystemCallError when the errno is libzmq-specific.
      #
      def syscall_error
        errno = L.zmq_errno
        SystemCallError.new(L.zmq_strerror(errno), errno)
      end


    end


    # Returns the shared ZMQ context (one per process, lazily initialized).
    #
    # @return [FFI::Pointer] zmq context pointer
    def self.context
      @context ||= Libzmq.zmq_ctx_new.tap do |ctx|
        raise "zmq_ctx_new failed" if ctx.null?
        at_exit do
          Libzmq.zmq_ctx_shutdown(ctx) rescue nil
          Libzmq.zmq_ctx_term(ctx) rescue nil
        end
      end
    end
  end
end
