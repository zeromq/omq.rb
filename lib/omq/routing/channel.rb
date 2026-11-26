# frozen_string_literal: true

module OMQ
  module Routing
    # CHANNEL socket routing: exclusive 1-to-1 bidirectional.
    #
    class Channel
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine        = engine
        @connection    = nil
        @recv_queue    = Routing.build_queue(engine.options.recv_hwm, :block)
        @send_queue    = nil
        @staging_queue = Routing.build_queue(engine.options.send_hwm, :block)
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
      # @raise [RuntimeError] if a connection already exists
      #
      def connection_added(connection)
        raise PeerRefusedError, "CHANNEL allows only one peer" if @connection
        @connection = connection

        # Single-frame socket: drop multipart input from a bad peer.
        @engine.start_recv_pump(connection, @recv_queue) { |msg| msg if msg.size == 1 }

        unless connection.is_a?(Transport::Inproc::Pipe)
          @send_queue = Routing.build_queue(@engine.options.send_hwm, :block)
          while (msg = @staging_queue.dequeue(timeout: 0))
            @send_queue.enqueue(msg)
          end
          start_send_pump(connection)
        end
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        if @connection == connection
          @connection = nil
          @send_queue = nil
        end
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        conn = @connection
        if conn.is_a?(Transport::Inproc::Pipe) && conn.direct_recv_queue
          conn.send_message(parts)
        elsif @send_queue
          @send_queue.enqueue(parts)
        else
          @staging_queue.enqueue(parts)
        end
      end


      # True when the staging and send queues are empty.
      #
      def send_queues_drained?
        @staging_queue.empty? && (@send_queue.nil? || @send_queue.empty?)
      end


      private


      def start_send_pump(conn)
        @engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          batch = []

          loop do
            Routing.dequeue_batch(@send_queue, batch) or break
            batch.each { |parts| conn.write_message(parts) }
            conn.flush
            batch.clear
            @engine.wake_drain
          end
        end
      end

    end
  end
end
