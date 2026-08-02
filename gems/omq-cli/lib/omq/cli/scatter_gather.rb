# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for SCATTER sockets (draft; fan-out send).
    class ScatterRunner < BaseRunner
      def run_loop(task) = run_send_logic
    end


    # Runner for GATHER sockets (draft; fan-in receive).
    class GatherRunner < BaseRunner
      def call(task)
        config.parallel ? run_parallel_workers(:GATHER) : super
      end


      private


      def run_loop(task) = run_recv_logic
    end
  end
end
