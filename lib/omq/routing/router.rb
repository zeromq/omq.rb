# frozen_string_literal: true

require "securerandom"
require "socket"

module OMQ
  module Routing
    # ROUTER socket routing: identity-based routing.
    #
    # Prepends peer identity frame on receive. Uses first frame as
    # routing identity on send. A dialed peer keeps its identity and
    # queue across reconnects ({IdentityRouting}).
    #
    class Router
      include ClosableQueues
      include IdentityRouting

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine          = engine
        @recv_queue      = Routing.build_queue(engine.options.recv_hwm, :block)
        @next_routing_id = SecureRandom.random_number(1 << 32)
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
        identity = attach_route(connection, requested_identity(connection)).id

        @engine.start_recv_pump(connection, @recv_queue) { |msg| [identity, *msg] }
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        detach_route(connection)
      end


      # Enqueues a message for sending. The first frame is the routing identity.
      #
      # Like libzmq, a ROUTER without +router_mandatory+ drops a message
      # whose peer is unknown or at HWM. With +router_mandatory+ it
      # raises for an unknown peer and blocks at HWM. A dialed peer that
      # is down is still known: its sends queue up.
      #
      # @param parts [Array<String>]
      #
      def enqueue(parts)
        identity  = parts.first
        mandatory = @engine.options.router_mandatory?
        route     = route_to(identity)

        unless route
          raise SocketError, "no route to identity #{identity.inspect}" if mandatory
          return
        end

        queue = route.queue
        return if !mandatory && queue.is_a?(Async::LimitedQueue) && queue.limited?

        Routing.enqueue_to_peer(queue, parts[1..])
      end


      private


      # The identity the peer set, unless empty or zero-prefixed (those
      # are reserved for generated ids).
      #
      def requested_identity(connection)
        identity = connection.peer_identity
        identity if identity && !identity.empty? && !identity.getbyte(0).zero?
      end


      # Routing id for a peer without a usable identity: a zero byte and
      # a 32-bit counter, as in libzmq. Peers can't claim identities
      # starting with a zero byte, so these never collide with theirs.
      #
      def generate_routing_id
        @next_routing_id = (@next_routing_id + 1) & 0xFFFF_FFFF
        [0, @next_routing_id].pack("CN")
      end

    end
  end
end
