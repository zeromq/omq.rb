# frozen_string_literal: true

module OMQ
  module Routing
    # PAIR socket routing: exclusive 1-to-1 bidirectional.
    #
    # Only one peer connection is allowed at a time. The send queue
    # is socket-level (one shared bounded queue), and a single send
    # pump fiber drains it into the connected peer. On disconnect,
    # the in-flight batch is dropped (matching libzmq).
    #
    class Pair
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine     = engine
        @connection = nil
        @recv_queue = Routing.build_queue(engine.options.recv_hwm, :block)
        @send_queue = Routing.build_queue(engine.options.send_hwm, :block)
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
        raise "PAIR allows only one peer" if @connection
        @connection = connection

        @engine.start_recv_pump(connection, @recv_queue)

        unless connection.is_a?(Transport::Inproc::Pipe)
          start_send_pump(connection)
        end
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connection = nil if @connection == connection
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        conn = @connection
        if conn.is_a?(Transport::Inproc::Pipe) && conn.direct_recv_queue
          conn.send_message(parts)
        else
          @send_queue.enqueue(parts)
        end
      end


      # @return [Boolean] true when the shared send queue is empty
      #
      def send_queues_drained?
        @send_queue.empty?
      end


      private


      def start_send_pump(conn)
        @engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          batch = []

          loop do
            Routing.dequeue_batch(@send_queue, batch)

            if batch.size == 1
              conn.write_message(batch.first)
            else
              conn.write_messages(batch)
            end

            conn.flush
            batch.each do |parts|
              @engine.emit_verbose_msg_sent(conn, parts)
            end
            batch.clear
          end
        end
      end

    end
  end
end
