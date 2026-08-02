# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for PUSH sockets (send-only pipeline producer).
    class PushRunner < BaseRunner
      def run_loop(task) = run_send_logic
    end


    # Runner for PULL sockets (receive-only pipeline consumer).
    class PullRunner < BaseRunner
      def call(task)
        config.parallel ? run_parallel_workers(:PULL) : super
      end


      private


      def run_loop(task) = run_recv_logic
    end
  end
end
