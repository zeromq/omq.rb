# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for RADIO sockets (draft; group-based publish).
    class RadioRunner < BaseRunner
      def run_loop(task) = run_send_logic


      private


      def send_msg(parts)
        case config.format
        when :marshal
          trace_send(parts)
          @sock.publish(config.group || "", Marshal.dump(parts))
        else
          return if parts.empty?
          trace_send(parts)
          group = config.group || parts.shift
          @sock.publish(group, parts.first || "")
        end
        transient_ready!
      end
    end


    # Runner for DISH sockets (draft; group-based subscribe).
    class DishRunner < BaseRunner
      def run_loop(task) = run_recv_logic
    end
  end
end
