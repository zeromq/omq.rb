# frozen_string_literal: true

require "securerandom"

module OMQ
  module Routing
    # PEER socket routing: bidirectional multi-peer with auto-generated
    # 4-byte routing IDs.
    #
    # Prepends routing ID on receive. Strips routing ID on send and
    # routes to the identified connection.
    #
    class Peer
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @return [Hash{String => Connection}] routing_id → connection
      #
      attr_reader :connections_by_routing_id


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine                     = engine
        @recv_queue                 = Routing.build_queue(engine.options.recv_hwm, :block)
        @connections_by_routing_id  = {}
        @routing_id_by_connection   = {}
        @conn_queues                = {}
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
        routing_id = SecureRandom.bytes(4)
        @connections_by_routing_id[routing_id] = connection
        @routing_id_by_connection[connection]  = routing_id

        # Single-frame socket: drop multipart input from a bad peer.
        @engine.start_recv_pump(connection, @recv_queue) { |msg| [routing_id, msg.first] if msg.size == 1 }

        q = Routing.build_queue(@engine.options.send_hwm, :block)
        @conn_queues[connection] = q
        ConnSendPump.start(@engine, connection, q)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        routing_id = @routing_id_by_connection.delete(connection)
        @connections_by_routing_id.delete(routing_id) if routing_id
        @conn_queues.delete(connection)
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        routing_id = parts.first
        conn = @connections_by_routing_id[routing_id]
        return unless conn
        @conn_queues[conn]&.enqueue(parts[1..])
      end


      # True when all per-connection send queues are empty.
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end

    end
  end
end
