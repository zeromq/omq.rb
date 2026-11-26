# frozen_string_literal: true

module OMQ
  module Routing
    # PUSH socket routing: round-robin send to PULL peers.
    #
    class Push
      include RoundRobin
      include ClosableQueues


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine = engine
        init_round_robin(engine)
      end


      # PUSH is write-only. Engine-facing recv contract: dequeue raises,
      # unblock is a no-op (fatal-error propagation still calls it).
      #
      def recv_queue
        raise "PUSH sockets cannot receive"
      end


      def dequeue_recv
        raise "PUSH sockets cannot receive"
      end


      def unblock_recv
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        add_round_robin_send_connection(connection)
        start_reaper(connection)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        remove_round_robin_send_connection(connection)
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        enqueue_round_robin(parts)
      end


      private


      # Detects peer disconnection on write-only sockets. Without
      # this, a dead peer is only noticed on the next send — which
      # may succeed if the kernel send buffer absorbs the data.
      #
      def start_reaper(conn)
        return if conn.is_a?(Transport::Inproc::Pipe)
        @engine.spawn_conn_pump_task(conn, annotation: "reaper") do
          conn.receive_message # blocks until peer disconnects; then exits
        end
      end

    end
  end
end
