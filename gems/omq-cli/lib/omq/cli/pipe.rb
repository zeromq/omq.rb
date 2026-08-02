# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for the virtual "pipe" socket type (PULL -> eval -> PUSH).
    # Supports sequential and parallel (Ractor-based) processing modes.
    class PipeRunner
      # @return [Config] frozen CLI configuration
      attr_reader :config


      # @param config [Config] frozen CLI configuration
      def initialize(config)
        @config = config
        @fmt    = Formatter.new(config.format)
      end


      # Runs the pipe in sequential or parallel mode based on config.
      #
      # @param task [Async::Task] the parent async task
      # @return [void]
      def call(task)
        if config.parallel
          run_parallel(task)
        else
          run_sequential(task)
        end
      end


      private


      def resolve_endpoints
        if config.in_endpoints.any?
          [config.in_endpoints, config.out_endpoints]
        else
          [[config.endpoints[0]], [config.endpoints[1]]]
        end
      end


      # -- Sequential ---------------------------------------------------


      def run_sequential(task)
        set_pipe_process_title
        log_backend
        in_eps, out_eps = resolve_endpoints
        @pull, @push = build_pull_push(in_eps, out_eps)
        compile_expr
        @sock = @pull  # for eval instance_exec
        start_event_monitors
        wait_for_peers_with_timeout if config.timeout
        setup_sequential_transient(task)
        @sock.instance_exec(&@recv_begin_proc) if @recv_begin_proc
        sequential_message_loop(fan_out: out_eps.size > 1)
        @sock.instance_exec(&@recv_end_proc) if @recv_end_proc
      rescue OMQ::SocketDeadError => error
        reason = error.cause&.message || error.message
        $stderr.write("omq: #{reason}\n")
        exit 1
      ensure
        @pull&.close
        @push&.close
      end


      # With --timeout set, fail fast if peers never show up. Without
      # it, there's no point waiting: PULL#receive blocks naturally
      # and PUSH buffers up to send_hwm when no peer is present.
      def wait_for_peers_with_timeout
        _, out_eps = resolve_endpoints
        Fiber.scheduler.with_timeout(config.timeout) do
          Barrier do |barrier|
            barrier.async(annotation: "wait pull peer") { @pull.peer_connected.wait }
            barrier.async(annotation: "wait push peers") do
              sleep 0.01 until @push.connection_count >= out_eps.size
            end
          end
        end
      end


      def build_pull_push(in_eps, out_eps)
        kwargs = SocketSetup.backend_kwargs(config)

        pull = OMQ::PULL.new(**kwargs).tap do |sock|
          SocketSetup.apply_options(sock, config)
          SocketSetup.apply_recv_maxsz(sock, config)
          SocketSetup.attach_endpoints sock, in_eps,
            config:     config,
            verbose:    config.verbose,
            timestamps: config.timestamps,
            side:       :in
        end

        push = OMQ::PUSH.new(**kwargs).tap do |sock|
          SocketSetup.apply_options(sock, config)
          SocketSetup.attach_endpoints sock, out_eps,
            config:     config,
            verbose:    config.verbose,
            timestamps: config.timestamps,
            side:       :out
        end

        [pull, push]
      end


      def setup_sequential_transient(task)
        return unless config.transient

        task.async do
          @pull.all_peers_gone.wait
          @pull.reconnect_enabled = false
          @pull.close_read
        end
      end


      def sequential_message_loop(fan_out: false)
        n = config.count
        i = 0

        loop do
          parts = @pull.receive or break
          parts = eval_recv_expr(parts)

          if parts && !parts.empty?
            @push.send(parts)
          end

          # Yield after send so send-pump fibers can drain the queue
          # before the next message is enqueued. Without this, one pump
          # monopolizes the shared queue via drain_send_queue_capped when
          # messages arrive in bursts (recv prefetch). Only needed for
          # multi-output pipes; single-output has no fairness concern.
          Async::Task.current.yield if fan_out

          i += 1

          if n && n > 0 && i >= n
            break
          end
        end
      end


      # -- Parallel -----------------------------------------------------


      def run_parallel(task)
        set_pipe_process_title
        log_backend
        OMQ.freeze_for_ractors!

        in_eps, out_eps      = resolve_endpoints
        in_eps               = RactorHelpers.preresolve_tcp(in_eps)
        out_eps              = RactorHelpers.preresolve_tcp(out_eps)
        log_port, log_thread = RactorHelpers.start_log_consumer
        error_port           = Ractor::Port.new
        error_thread         = Thread.new(error_port) do |p|
          msg = p.receive
          abort "omq: #{msg}" unless msg.equal?(RactorHelpers::SHUTDOWN)
        rescue Ractor::ClosedError
          # port closed, no error
        end

        workers = config.parallel.times.map do
          ::Ractor.new(config, in_eps, out_eps, log_port, error_port) do |cfg, ins, outs, lport, eport|
            PipeWorker.new(cfg, ins, outs, lport, eport).call
          end
        end

        workers.each do |w|
          w.join
        rescue ::Ractor::RemoteError => e
          $stderr.write("omq: Ractor error: #{e.cause&.message || e.message}\n")
        end
      ensure
        RactorHelpers.stop_consumer(error_port, error_thread) if error_port
        RactorHelpers.stop_consumer(log_port, log_thread) if log_port
      end


      # -- Process title -------------------------------------------------


      def set_pipe_process_title
        in_eps, out_eps = resolve_endpoints
        in_flag  = SocketSetup.compress_flag(config, side: :in)
        out_flag = SocketSetup.compress_flag(config, side: :out)
        title = ["omq pipe"]
        title << "-P#{config.parallel}" if config.parallel
        if in_flag && in_flag == out_flag
          title << in_flag
          title.concat(in_eps.map(&:url))
          title << "->"
          title.concat(out_eps.map(&:url))
        else
          title << "--in" << in_flag if in_flag
          title.concat(in_eps.map(&:url))
          title << "->"
          title << out_flag if out_flag
          title.concat(out_eps.map(&:url))
        end

        Process.setproctitle(title.join(" "))
      end


      # -- Expression eval ----------------------------------------------


      def compile_expr
        @recv_evaluator  = ExpressionEvaluator.new(config.recv_expr, format: config.format)
        @recv_begin_proc = @recv_evaluator.begin_proc
        @recv_eval_proc  = @recv_evaluator.eval_proc
        @recv_end_proc   = @recv_evaluator.end_proc
      end


      def eval_recv_expr(parts)
        result = @recv_evaluator.call(parts, @sock)
        result.equal?(ExpressionEvaluator::SENT) ? nil : result
      end


      # -- Event monitoring ---------------------------------------------


      def start_event_monitors
        trace      = config.verbose >= 3
        log_events = config.verbose >= 2
        [@pull, @push].each do |sock|
          sock.monitor(verbose: trace) do |event|
            Term.write_event(event, config.timestamps) if log_events
            SocketSetup.kill_on_protocol_error(sock, event)
          end
        end
      end


      def log_backend
        return unless config.verbose >= 1
        $stderr.write("#{Term.log_prefix(config.timestamps)}omq: backend: #{SocketSetup.backend_name(config)}\n")
      end

    end
  end
end
