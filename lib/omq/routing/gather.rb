# frozen_string_literal: true

module OMQ
  module Routing
    # GATHER socket routing: fair-queue receive from SCATTER peers.
    #
    class Gather
      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine     = engine
        @recv_queue = Routing.build_queue(engine.options.recv_hwm, :block)
      end


      # Dequeues the next received message. Blocks until one is available.
      #
      # @return [Array<String>, nil]
      #
      def dequeue_recv
        @recv_queue.dequeue
      end


      # Wakes a blocked {#dequeue_recv} with a nil sentinel.
      #
      # @return [void]
      #
      def unblock_recv
        @recv_queue.enqueue(nil)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        # Single-frame socket: drop multipart input from a bad peer.
        @engine.start_recv_pump(connection, @recv_queue) { |msg| msg if msg.size == 1 }
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
      end


      # GATHER is read-only.
      #
      def enqueue(_parts)
        raise "GATHER sockets cannot send"
      end

    end
  end
end
