# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for ROUTER sockets (identity-aware async routing).
    class RouterRunner < BaseRunner
      include RoutingHelper

      private


      def run_loop(task)
        receiver = recv_async(task)
        sender   = async_send_loop(task)
        wait_for_loops(receiver, sender)
      end


      def recv_async(task)
        task.async do
          n = config.count
          i = 0
          loop do
            parts = recv_msg_raw
            break if parts.nil?
            identity = parts.shift
            parts.shift if parts.first == ""
            result = eval_recv_expr([display_routing_id(identity), *parts])
            output(result)
            i += 1
            break if n && n > 0 && i >= n
          end
        end
      end


      def send_to_peer(id, parts)
        @sock.send([id, "", *parts])
      end
    end
  end
end
