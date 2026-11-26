# frozen_string_literal: true

module OMQ
  module Routing
    # Starts a dedicated send pump for one per-connection send queue.
    #
    # Used by {IdentityRouting} (ROUTER, SERVER, PEER) and Rep, which
    # have per-peer queues but do not include the RoundRobin mixin.
    #
    module ConnSendPump
      # Spawns the pump task on the connection's lifecycle barrier so it
      # is torn down with the rest of the connection's pumps.
      #
      # @param engine [Engine]
      # @param conn [Protocol::ZMTP::Connection]
      # @param q [Async::LimitedQueue]
      # @return [Async::Task]
      #
      def self.start(engine, conn, q)
        engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          batch = []

          loop do
            Routing.dequeue_batch(q, batch) or break

            if batch.size == 1
              conn.write_message batch.first
            else
              conn.write_messages batch
            end

            conn.flush

            batch.each do |parts|
              engine.emit_verbose_msg_sent(conn, parts)
            end

            batch.clear
            engine.wake_drain
          end
        end
      end

    end
  end
end
