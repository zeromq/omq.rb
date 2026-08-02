# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for REQ sockets (synchronous request-reply client).
    class ReqRunner < BaseRunner
      private


      def run_loop(task)
        n = config.count
        i = 0

        sleep config.delay if config.delay
        generator = @send_eval_proc && !config.data && !config.file && !stdin_ready?

        loop do
          if generator
            parts = eval_send_expr(nil)
          else
            parts = read_next
            break unless parts
            parts = eval_send_expr(parts)
          end

          next unless parts

          send_msg(parts)
          reply = recv_msg or break
          output(eval_recv_expr(reply))
          i += 1
          break if n && n > 0 && i >= n
          break if !config.interval && (generator || config.data || config.file) && !(n && n > 0)
          wait_for_interval if config.interval
        end
      end


      def wait_for_interval
        wait = config.interval - (Time.now.to_f % config.interval)
        sleep(wait) if wait > 0
      end
    end


    # Runner for REP sockets (synchronous request-reply server).
    class RepRunner < BaseRunner
      def call(task)
        if config.parallel
          run_parallel_workers(:REP)
        else
          super
        end
      end


      private


      def run_loop(task)
        n = config.count
        i = 0

        loop do
          msg = recv_msg or break
          break unless handle_rep_request(msg)
          i += 1
          break if n && n > 0 && i >= n
        end
      end


      def handle_rep_request(msg)
        if config.recv_expr || @recv_eval_proc
          reply = eval_recv_expr(msg)

          unless reply.equal?(SENT)
            output(reply)
            send_msg(reply || [""])
          end
        elsif config.echo
          output(msg)
          send_msg(msg)
        elsif config.data || config.file || !config.stdin_is_tty
          reply = read_next
          return false unless reply
          output(msg)
          send_msg(reply)
        else
          abort "REP needs a reply source: --echo, --data, --file, -e, or stdin pipe"
        end

        true
      end
    end
  end
end
