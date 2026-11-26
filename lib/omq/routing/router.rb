# frozen_string_literal: true

require "securerandom"
require "socket"

module OMQ
  module Routing
    # ROUTER socket routing: identity-based routing.
    #
    # Prepends peer identity frame on receive. Uses first frame as
    # routing identity on send.
    #
    class Router
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine                  = engine
        @recv_queue              = Routing.build_queue(engine.options.recv_hwm, :block)
        @connections_by_identity = {}
        @identity_by_connection  = {}
        @next_routing_id         = SecureRandom.random_number(1 << 32)
        @conn_queues             = {}
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
        identity = connection.peer_identity
        identity = generate_identity if identity.nil? || identity.empty? || identity.getbyte(0).zero?
        @connections_by_identity[identity] = connection
        @identity_by_connection[connection] = identity

        @engine.start_recv_pump(connection, @recv_queue) { |msg| [identity, *msg] }

        q = Routing.build_queue(@engine.options.send_hwm, :block)
        @conn_queues[connection] = q
        ConnSendPump.start(@engine, connection, q)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        identity = @identity_by_connection.delete(connection)

        # A newer peer may have taken over this identity. Only drop the
        # route if it still points at the departing connection.
        if identity && @connections_by_identity[identity].equal?(connection)
          @connections_by_identity.delete(identity)
        end

        @conn_queues.delete(connection)&.close
      end


      # Enqueues a message for sending. The first frame is the routing identity.
      #
      # Like libzmq, a ROUTER without +router_mandatory+ drops a message
      # whose peer is unknown or at HWM. With +router_mandatory+ it
      # raises for an unknown peer and blocks at HWM.
      #
      # @param parts [Array<String>]
      #
      def enqueue(parts)
        identity  = parts.first
        mandatory = @engine.options.router_mandatory?

        if mandatory
          unless @connections_by_identity[identity]
            raise SocketError, "no route to identity #{identity.inspect}"
          end
        end

        conn = @connections_by_identity[identity]
        return unless conn  # silently drop if peer disconnected

        queue = @conn_queues[conn]
        return if !mandatory && queue.is_a?(Async::LimitedQueue) && queue.limited?

        Routing.enqueue_to_peer(queue, parts[1..])
      end


      # @return [Boolean] true when all per-connection send queues are empty
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end


      private


      # Routing id for a peer without a usable identity: a zero byte and
      # a 32-bit counter, as in libzmq. Peers can't claim identities
      # starting with a zero byte, so these never collide with theirs.
      #
      def generate_identity
        @next_routing_id = (@next_routing_id + 1) & 0xFFFF_FFFF
        [0, @next_routing_id].pack("CN")
      end

    end
  end
end
