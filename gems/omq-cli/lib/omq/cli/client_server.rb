# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for SERVER and PEER sockets (draft; routing-id-based messaging).
    class ServerRunner < BaseRunner
      include RoutingHelper

      private


      def run_loop(task)
        if config.echo || config.recv_expr || @recv_eval_proc || config.data || config.file || !config.stdin_is_tty
          reply_loop
        else
          monitor_loop(task)
        end
      end


      def reply_loop
        n = config.count
        i = 0
        loop do
          parts = recv_msg_raw
          break if parts.nil?
          routing_id = parts.shift
          break unless handle_server_request(routing_id, parts)
          i += 1
          break if n && n > 0 && i >= n
        end
      end


      def handle_server_request(routing_id, body)
        if config.recv_expr || @recv_eval_proc
          reply = eval_recv_expr(body)
          output([display_routing_id(routing_id), *(reply || [""])])
          @sock.send_to(routing_id, (reply || [""]).first)
        elsif config.echo
          output([display_routing_id(routing_id), *body])
          @sock.send_to(routing_id, body.first || "")
        elsif config.data || config.file || !config.stdin_is_tty
          reply = read_next
          return false unless reply
          output([display_routing_id(routing_id), *body])
          @sock.send_to(routing_id, reply.first || "")
        end
        true
      end


      def monitor_loop(task)
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
            routing_id = parts.shift
            result = eval_recv_expr([display_routing_id(routing_id), *parts])
            output(result)
            i += 1
            break if n && n > 0 && i >= n
          end
        end
      end


      def send_to_peer(id, parts)
        @sock.send_to(id, parts.first || "")
      end
    end
  end
end
