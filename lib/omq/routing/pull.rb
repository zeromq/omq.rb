# frozen_string_literal: true

module OMQ
  module Routing
    # PULL socket routing: fair-queue receive from PUSH peers.
    #
    class Pull
      include ClosableQueues

      # @param engine [Engine]
      #
      def initialize(engine)
        @engine     = engine
        @recv_queue = Routing.build_queue(engine.options.recv_hwm, :block)
      end


      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # Dequeues the next received message. Blocks until one is available.
      # Engine-facing contract — Engine must not touch @recv_queue directly.
      #
      # @return [Array<String>, nil]
      #
      def dequeue_recv
        @recv_queue.dequeue
      end


      # Wakes a blocked {#dequeue_recv} with a nil sentinel. Called by
      # Engine on close or fatal-error propagation.
      #
      # @return [void]
      #
      def unblock_recv
        @recv_queue.enqueue(nil)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        @engine.start_recv_pump(connection, @recv_queue)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        # recv pump stops on EOFError via its connection barrier
      end


      # PULL is read-only.
      #
      def enqueue(_parts)
        raise "PULL sockets cannot send"
      end

    end
  end
end
