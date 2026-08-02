# frozen_string_literal: true

module OMQ
  module CLI
    # Template runner base class for all socket-type CLI runners.
    # Subclasses override {#run_loop} to implement socket-specific behaviour.
    class BaseRunner
      # @return [Config] frozen CLI configuration
      # @return [Object] the OMQ socket instance
      attr_reader :config, :sock


      # @param config [Config] frozen CLI configuration
      # @param socket_class [Class] OMQ socket class to instantiate (e.g. OMQ::PUSH)
      def initialize(config, socket_class)
        @config = config
        @klass  = socket_class
        @fmt    = Formatter.new(config.format)
      end


      # Runs the full lifecycle: socket setup, peer wait, BEGIN/END blocks, and the main loop.
      #
      # @param task [Async::Task] the parent async task
      # @return [void]
      def call(task)
        set_process_title
        log_backend
        setup_socket
        start_event_monitor
        maybe_start_transient_monitor(task)
        sleep(config.delay) if config.delay && config.recv_only?
        wait_for_peer if needs_peer_wait?
        run_begin_blocks
        run_loop(task)
        run_end_blocks
      rescue OMQ::SocketDeadError => error
        reason = error.cause&.message || error.message
        $stderr.write("omq: #{reason}\n")
        exit 1
      ensure
        @sock&.close
      end


      private


      # Subclasses override this.
      def run_loop(task)
        raise NotImplementedError
      end


      # -- Parallel Ractor workers -----------------------------------------


      def run_parallel_workers(socket_sym)
        OMQ.freeze_for_ractors!
        eps = RactorHelpers.preresolve_tcp(config.endpoints)
        output_port, output_thread = RactorHelpers.start_output_consumer
        log_port, log_thread = RactorHelpers.start_log_consumer
        error_port = Ractor::Port.new
        error_thread = Thread.new(error_port) do |p|
          msg = p.receive
          abort "omq: #{msg}" unless msg.equal?(RactorHelpers::SHUTDOWN)
        rescue Ractor::ClosedError
          # port closed, no error
        end

        workers = config.parallel.times.map do
          ::Ractor.new(config, socket_sym, eps, output_port, log_port, error_port) do |cfg, sym, e, oport, lport, eport|
            ParallelWorker.new(cfg, sym, e, oport, lport, eport).call
          end
        end

        workers.each do |w|
          w.join
        rescue ::Ractor::RemoteError => e
          $stderr.write("omq: Ractor error: #{e.cause&.message || e.message}\n")
        end
      ensure
        RactorHelpers.stop_consumer(error_port, error_thread) if error_port
        RactorHelpers.stop_consumer(output_port, output_thread) if output_port
        RactorHelpers.stop_consumer(log_port, log_thread) if log_port
      end


      # -- Socket creation ---------------------------------------------


      def setup_socket
        @sock = create_socket
        attach_endpoints
        setup_curve
        setup_subscriptions
        compile_expr
      end


      def create_socket
        SocketSetup.build(@klass, config)
      end


      def attach_endpoints
        SocketSetup.attach(@sock, config, verbose: config.verbose, timestamps: config.timestamps)
      end


      # -- Transient disconnect monitor --------------------------------


      def maybe_start_transient_monitor(task)
        return unless config.transient
        @transient_monitor = TransientMonitor.new(@sock, config, task, method(:log))
        Async::Task.current.yield  # let monitor start waiting
      end


      def transient_ready!
        @transient_monitor&.ready!
      end


      # -- BEGIN / END blocks ------------------------------------------


      def run_begin_blocks
        @sock.instance_exec(&@send_begin_proc) if @send_begin_proc
        @sock.instance_exec(&@recv_begin_proc) if @recv_begin_proc
      end


      def run_end_blocks
        @sock.instance_exec(&@send_end_proc) if @send_end_proc
        @sock.instance_exec(&@recv_end_proc) if @recv_end_proc
      end


      # -- Peer wait with grace period ---------------------------------


      def needs_peer_wait?
        return false if config.recv_only?
        return true if config.connects.any?
        return true if config.type_name == "router"

        # Bind-mode senders with a bounded or scheduled send plan:
        # wait for the first peer so a one-shot `-d` / `-E` doesn't
        # just queue into HWM and then exit before anyone is
        # listening. Interactive stdin still goes through unwaited
        # so typing isn't gated on a peer.
        config.binds.any? && bounded_or_scheduled_send?
      end


      def bounded_or_scheduled_send?
        return true if config.interval || config.data || config.file || @send_eval_proc
        # Piped stdin (non-tty) is also a bounded send plan: we read
        # until EOF and exit. Without a peer wait, a bind-mode sender
        # dumps straight into HWM and closes before any peer connects.
        !config.stdin_is_tty
      end


      def wait_for_peer
        wait_body = proc do
          @sock.peer_connected.wait
          log "peer connected"
          wait_for_subscriber
          apply_grace_period
        end

        if config.timeout
          Fiber.scheduler.with_timeout(config.timeout, &wait_body)
        else
          wait_body.call
        end
      end


      def wait_for_subscriber
        return unless %w[pub xpub].include?(config.type_name)
        @sock.subscriber_joined.wait
        log "subscriber joined"
      end


      # Grace period: when multiple peers may be connecting (bind or
      # multiple connect URLs), wait one reconnect interval so
      # latecomers finish their handshake before we start sending.
      def apply_grace_period
        return unless config.binds.any? || config.connects.size > 1
        ri = @sock.options.reconnect_interval
        sleep(ri.is_a?(Range) ? ri.begin : ri)
      end


      # -- Socket setup ------------------------------------------------


      def setup_subscriptions
        SocketSetup.setup_subscriptions(@sock, config)
      end


      def setup_subscriptions_on(sock)
        SocketSetup.setup_subscriptions(sock, config)
      end


      def setup_curve
        SocketSetup.setup_curve(@sock, config)
      end


      # -- Shared loop bodies ------------------------------------------


      def run_send_logic
        n = config.count
        sleep(config.delay) if config.delay
        if config.interval
          run_interval_send(n)
        elsif config.data || config.file
          # One-shot from -d/-f. --count N fires the same payload N times.
          parts = eval_send_expr(read_next)
          (n && n > 0 ? n : 1).times { send_msg(parts) } if parts
        elsif stdin_ready?
          run_stdin_send(n)
        elsif @send_eval_proc
          # Pure generator: -e/-E with no stdin input. Fire once by
          # default, --count N fires N times.
          (n && n > 0 ? n : 1).times do
            parts = eval_send_expr(nil)
            send_msg(parts) if parts
          end
        elsif config.stdin_is_tty
          # Bare interactive invocation on a terminal: read lines from
          # the tty until the user hits ^D.
          run_stdin_send(n)
        end
      end


      def run_interval_send(n)
        i = send_tick
        return if @send_tick_eof || (n && n > 0 && i >= n)
        Async::Loop.quantized(interval: config.interval) do
          i += send_tick
          break if @send_tick_eof || (n && n > 0 && i >= n)
        end
      end


      def run_stdin_send(n)
        i = 0
        loop do
          parts = read_next
          break unless parts
          parts = eval_send_expr(parts)
          send_msg(parts) if parts
          i += 1
          break if n && n > 0 && i >= n
        end
      end


      def send_tick
        raw = read_next_or_nil
        if raw.nil?
          if @send_eval_proc && !@stdin_ready
            # Pure generator mode: no stdin, eval produces output from nothing.
            parts = eval_send_expr(nil)
            send_msg(parts) if parts
            return 1
          end
          @send_tick_eof = true
          return 0
        end
        parts = eval_send_expr(raw)
        send_msg(parts) if parts
        1
      end


      def run_recv_logic
        n = config.count
        i = 0
        if config.interval
          run_interval_recv(n)
        else
          loop do
            parts = recv_msg
            break if parts.nil?
            parts = eval_recv_expr(parts)
            output(parts)
            i += 1
            break if n && n > 0 && i >= n
          end
        end
      end


      def run_interval_recv(n)
        i = recv_tick
        return if i == 0
        return if n && n > 0 && i >= n
        Async::Loop.quantized(interval: config.interval) do
          i += recv_tick
          break if @recv_tick_eof || (n && n > 0 && i >= n)
        end
      end


      def recv_tick
        parts = recv_msg
        if parts.nil?
          @recv_tick_eof = true
          return 0
        end
        parts = eval_recv_expr(parts)
        output(parts)
        1
      end


      # At -vvv, log the received message *before* eval runs. Eval
      # may write to stdout (e.g. `-e 'p it'`), and we want the
      # trace line to precede any such output so the sequence on the
      # terminal reads as: trace → eval side-effects → body.
      #
      # +@last_recv_wire_size+ is populated by the :message_received
      # monitor event, which fires *before* the recv queue enqueue
      # (recv_pump.rb) — so by the time @sock.receive returns here,
      # the cache reflects this message. +@last_recv_uncompressed+
      # is captured in #recv_msg from the raw marshal frame size.
      def trace_recv(parts)
        return unless config.verbose >= 3
        preview = Formatter.preview(parts,
                                    format:            config.format,
                                    wire_size:         @last_recv_wire_size,
                                    uncompressed_size: @last_recv_uncompressed)
        $stderr.write("#{Term.log_prefix(config.timestamps)}omq: << #{preview}\n")
        $stderr.flush
      end


      def wait_for_loops(receiver, sender)
        if config.data || config.file || config.send_expr || config.recv_expr || config.target
          sender.wait
          receiver.stop
        elsif config.count && config.count > 0
          receiver.wait
          sender.stop
        else
          sender.wait
          receiver.stop
        end
      end


      # -- Message I/O -------------------------------------------------


      def send_msg(parts)
        case config.format
        when :marshal
          dumped = Marshal.dump(parts)
          trace_send(parts, uncompressed_size: dumped.bytesize)
          @sock.send([dumped])
        else
          return if parts.empty?
          trace_send(parts)
          @sock.send(parts)
        end
        transient_ready!
      end


      # Symmetric to #trace_recv — log the outgoing message using the
      # pre-Marshal.dump +parts+, so -M traces show the app-level
      # object (`[nil, :foo, "bar"]`) instead of the wire-side dump
      # bytes. +@last_send_wire_size+ is best-effort: it reflects the
      # *previous* message (populated by the :message_sent monitor
      # event, which fires on a separate fiber after the pump writes),
      # so early sends may show no `wire=` at all. Receive-side tracing
      # is the authoritative path for observing wire bytes.
      def trace_send(parts, uncompressed_size: nil)
        return unless config.verbose >= 3
        preview = Formatter.preview(parts,
                                    format:            config.format,
                                    wire_size:         @last_send_wire_size,
                                    uncompressed_size: uncompressed_size)
        $stderr.write("#{Term.log_prefix(config.timestamps)}omq: >> #{preview}\n")
        $stderr.flush
      end


      def recv_msg
        parts = @sock.receive
        return nil if parts.nil?

        case config.format
        when :marshal
          @last_recv_uncompressed = parts.first.bytesize
          parts = Marshal.load(parts.first)
        end

        trace_recv(parts)
        transient_ready!
        parts
      end


      def recv_msg_raw
        msg = @sock.receive
        msg&.dup
      end


      def read_next
        config.data || config.file ? read_inline_data : read_stdin_input
      end


      def read_inline_data
        if config.data
          @inline_data ||= @fmt.decode(config.data + "\n")
        else
          @file_data ||= (config.file == "-" ? $stdin.read : File.read(config.file)).chomp
          @fmt.decode(@file_data + "\n")
        end
      end


      def read_stdin_input
        case config.format
        when :msgpack
          @fmt.decode_msgpack($stdin)
        when :marshal
          @fmt.decode_marshal($stdin)
        when :raw
          data = $stdin.read
          data.nil? || data.empty? ? nil : [data]
        else
          # Skip blank input lines — they are a no-op "wait for next
          # line" rather than a zero-frame message. Stops REQ from
          # wedging in recv after an accidental Enter on a tty, and
          # stops PUSH/etc. from silently dropping the iteration via
          # send_msg's `return if parts.empty?` guard.
          loop do
            line = $stdin.gets
            return nil if line.nil?
            next if line.chomp.empty?
            return @fmt.decode(line)
          end
        end
      end


      def stdin_ready?
        return @stdin_ready unless @stdin_ready.nil?

        @stdin_ready = !$stdin.closed? &&
                       !config.stdin_is_tty &&
                       IO.select([$stdin], nil, nil, 0.01) &&
                       !$stdin.eof?
      end


      def read_next_or_nil
        if config.data || config.file
          read_next
        elsif stdin_ready?
          read_stdin_input
        else
          nil
        end
      end


      def output(parts)
        return if config.quiet || parts.nil?
        $stdout.write(@fmt.encode(parts))
        $stdout.flush
      end


      # -- Eval --------------------------------------------------------


      def compile_expr
        @send_evaluator = compile_evaluator(config.send_expr, fallback: OMQ.outgoing_proc)
        @recv_evaluator = compile_evaluator(config.recv_expr, fallback: OMQ.incoming_proc)
        assign_send_aliases
        assign_recv_aliases
      end


      def compile_evaluator(src, fallback:)
        ExpressionEvaluator.new(src, format: config.format, fallback_proc: fallback)
      end


      def assign_send_aliases
        # Keep ivar aliases -- subclasses check these directly
        @send_begin_proc = @send_evaluator.begin_proc
        @send_eval_proc  = @send_evaluator.eval_proc
        @send_end_proc   = @send_evaluator.end_proc
      end


      def assign_recv_aliases
        @recv_begin_proc = @recv_evaluator.begin_proc
        @recv_eval_proc  = @recv_evaluator.eval_proc
        @recv_end_proc   = @recv_evaluator.end_proc
      end


      def eval_send_expr(parts)
        @send_evaluator.call(parts, @sock)
      end


      def eval_recv_expr(parts)
        @recv_evaluator.call(parts, @sock)
      end


      SENT = ExpressionEvaluator::SENT


      # -- Process title -------------------------------------------------


      def set_process_title(endpoints: nil)
        eps = endpoints || config.endpoints
        title = ["omq", config.type_name]
        if (flag = SocketSetup.compress_flag(config))
          title << flag
        end
        title << "-P#{config.parallel}" if config.parallel
        eps.each do |ep|
          title << (ep.respond_to?(:url) ? ep.url : ep.to_s)
        end
        Process.setproctitle(title.join(" "))
      end


      # -- Logging -----------------------------------------------------


      def log(msg)
        return unless config.verbose >= 1
        $stderr.write("#{Term.log_prefix(config.timestamps)}omq: #{msg}\n")
      end


      def log_backend
        log("backend: #{SocketSetup.backend_name(config)}")
      end


      # Always attached so protocol-level disconnect events can kill
      # the socket. Verbose gating lives inside the callback:
      #   -vv  log connect/disconnect/retry/timeout events
      #   -vvv also log message sent/received traces
      # --timestamps[=s|ms|us]: prepend UTC timestamps to log lines
      #
      # :message_received and :message_sent are not *logged* from the
      # monitor fiber — #trace_recv / #trace_send render them inline
      # on the same fiber as the body write, so trace-then-body
      # ordering is strict on a shared tty. The monitor-fiber path
      # suffered from $stderr/$stdout buffer races and from dumping
      # wire-side bytes (pre-Marshal.load on recv, post-Marshal.dump
      # on send) instead of app-level parts. We still *observe* these
      # events here to side-channel the compressed wire_size — for
      # :message_received the event fires before the recv queue
      # enqueue (engine/recv_pump.rb), so by the time @sock.receive
      # returns, @last_recv_wire_size reflects the current message.
      def start_event_monitor
        trace      = config.verbose >= 3
        log_events = config.verbose >= 2
        @sock.monitor(verbose: trace) do |event|
          case event.type
          when :message_received
            @last_recv_wire_size = event.detail[:wire_size]
          when :message_sent
            @last_send_wire_size = event.detail[:wire_size]
          else
            Term.write_event(event, config.timestamps) if log_events
          end
          SocketSetup.kill_on_protocol_error(@sock, event)
        end
      end
    end
  end
end
