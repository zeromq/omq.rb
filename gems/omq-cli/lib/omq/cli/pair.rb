# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for PAIR, DEALER, and CHANNEL sockets (bidirectional messaging).
    class PairRunner < BaseRunner
      private


      def run_loop(task)
        receiver = recv_async(task)
        sender   = task.async { run_send_logic }
        wait_for_loops(receiver, sender)
      end


      def recv_async(task)
        task.async do
          n = config.count
          i = 0
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
    end
  end
end
