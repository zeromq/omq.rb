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
        identity = SecureRandom.bytes(5) if identity.nil? || identity.empty?
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

        @conn_queues.delete(connection)
      end


      # Enqueues a message for sending. The first frame is the routing identity.
      #
      # @param parts [Array<String>]
      #
      def enqueue(parts)
        identity = parts.first
        if @engine.options.router_mandatory?
          unless @connections_by_identity[identity]
            raise SocketError, "no route to identity #{identity.inspect}"
          end
        end
        conn = @connections_by_identity[identity]
        return unless conn  # silently drop if peer disconnected
        @conn_queues[conn]&.enqueue(parts[1..])
      end


      # @return [Boolean] true when all per-connection send queues are empty
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end

    end
  end
end
