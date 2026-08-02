# frozen_string_literal: true

module OMQ
  module CLI
    # Worker that runs inside a Ractor for pipe -P parallel mode.
    # Each worker owns its own Async reactor, PULL socket, and PUSH socket.
    #
    class PipeWorker
      def initialize(config, in_eps, out_eps, log_port, error_port = nil)
        @config     = config
        @in_eps     = in_eps
        @out_eps    = out_eps
        @log_port   = log_port
        @error_port = error_port
      end


      def call
        Async do
          setup_sockets
          log_endpoints if @config.verbose >= 1
          start_monitors
          wait_for_peers_with_timeout if @config.timeout
          compile_expr
          run_message_loop
          run_end_block
        rescue OMQ::SocketDeadError => error
          reason = error.cause&.message || error.message
          @log_port.send("omq: #{reason}")
        ensure
          @pull&.close
          @push&.close
        end
      end


      private


      def setup_sockets
        kwargs = OMQ::CLI::SocketSetup.backend_kwargs(@config)
        @pull = OMQ::PULL.new(**kwargs)
        @push = OMQ::PUSH.new(**kwargs)
        OMQ::CLI::SocketSetup.apply_options(@pull, @config)
        OMQ::CLI::SocketSetup.apply_options(@push, @config)
        OMQ::CLI::SocketSetup.apply_recv_maxsz(@pull, @config)
        OMQ::CLI::SocketSetup.attach_endpoints(@pull, @in_eps, config: @config, verbose: 0, side: :in)
        OMQ::CLI::SocketSetup.attach_endpoints(@push, @out_eps, config: @config, verbose: 0, side: :out)
      end


      def log_endpoints
        (@in_eps + @out_eps).each do |ep|
          @log_port.send(OMQ::CLI::Term.format_attach(ep.bind? ? :bind : :connect, ep.url, @config.timestamps))
        end
      end


      def start_monitors
        trace      = @config.verbose >= 3
        log_events = @config.verbose >= 2
        [@pull, @push].each do |sock|
          sock.monitor(verbose: trace) do |event|
            @log_port.send(OMQ::CLI::Term.format_event(event, @config.timestamps)) if log_events
            kill_on_protocol_error(sock, event)
          end
        end
      end


      def kill_on_protocol_error(sock, event)
        return unless event.type == :disconnected
        error = event.detail && event.detail[:error]
        return unless error.is_a?(Protocol::ZMTP::Error)
        sock.engine.signal_fatal_error(error)
      end


      # With --timeout set, fail fast if peers never show up. Without
      # it, there's no point waiting: PULL#receive blocks naturally
      # and PUSH buffers up to send_hwm when no peer is present.
      def wait_for_peers_with_timeout
        Fiber.scheduler.with_timeout(@config.timeout) do
          Barrier do |barrier|
            barrier.async { @pull.peer_connected.wait }
            barrier.async { @push.peer_connected.wait }
          end
        end
      end


      def compile_expr
        @begin_proc, @end_proc, @eval_proc =
          OMQ::CLI::ExpressionEvaluator.compile_inside_ractor(@config.recv_expr)
        @fmt = OMQ::CLI::Formatter.new(@config.format)
        @ctx = Object.new
        @ctx.instance_exec(&@begin_proc) if @begin_proc
      end


      def run_message_loop
        n = @config.count
        if @eval_proc
          if n && n > 0
            n.times { break unless process_one_eval }
          else
            loop { break unless process_one_eval }
          end
        else
          if n && n > 0
            n.times { break unless process_one_passthrough }
          else
            loop { break unless process_one_passthrough }
          end
        end
      rescue IO::TimeoutError, Async::TimeoutError
        # recv timed out -- fall through to END block
      end


      def process_one_eval
        parts_in = @pull.receive
        return false if parts_in.nil?
        parts_out = OMQ::CLI::ExpressionEvaluator.normalize_result(
          @ctx.instance_exec(parts_in, &@eval_proc), format: @config.format
        )
        return true if parts_out.nil? || parts_out.empty?
        @push << parts_out
        true
      end


      def process_one_passthrough
        parts_in = @pull.receive
        return false if parts_in.nil?
        @push << parts_in
        true
      end


      def run_end_block
        return unless @end_proc
        out = OMQ::CLI::ExpressionEvaluator.normalize_result(
          @ctx.instance_exec(&@end_proc), format: @config.format
        )
        @push << out if out && !out.empty?
      end
    end
  end
end
