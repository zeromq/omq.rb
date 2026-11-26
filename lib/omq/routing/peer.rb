# frozen_string_literal: true

require "securerandom"

module OMQ
  module Routing
    # PEER socket routing: bidirectional multi-peer with auto-generated
    # 4-byte routing IDs.
    #
    # Prepends routing ID on receive. Strips routing ID on send and
    # routes to the identified connection. A dialed peer keeps its
    # routing ID and queue across reconnects ({IdentityRouting}).
    #
    class Peer
      include ClosableQueues
      include IdentityRouting

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine     = engine
        @recv_queue = Routing.build_queue(engine.options.recv_hwm, :block)
        init_identity_routing(engine)
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
        routing_id = attach_route(connection).id

        # Single-frame socket: drop multipart input from a bad peer.
        @engine.start_recv_pump(connection, @recv_queue) { |msg| [routing_id, msg.first] if msg.size == 1 }
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        detach_route(connection)
      end


      # @return [Hash{String => Connection}] routing_id → connected peer
      #
      def connections_by_routing_id
        @routes.each_with_object({}) do |(id, route), map|
          map[id] = route.conn if route.conn
        end
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        route = route_to(parts.first)
        return unless route

        Routing.enqueue_to_peer(route.queue, parts[1..])
      end


      private


      def generate_routing_id
        SecureRandom.bytes(4)
      end

    end
  end
end
