# frozen_string_literal: true

module OMQ
  module CLI
    # Runner for PUB sockets (publish messages to subscribers).
    class PubRunner < BaseRunner
      def run_loop(task) = run_send_logic
    end


    # Runner for SUB sockets (subscribe and receive published messages).
    class SubRunner < BaseRunner
      def run_loop(task) = run_recv_logic
    end
  end
end
