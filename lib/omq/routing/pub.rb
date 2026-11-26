# frozen_string_literal: true

module OMQ
  module Routing
    # PUB socket routing: fan-out to all subscribers.
    #
    # Listens for SUBSCRIBE/CANCEL commands from peers.
    # Each subscriber gets its own bounded send queue; slow subscribers
    # are muted via the socket's on_mute strategy (drop by default).
    #
    class Pub
      include FanOut
      include ClosableQueues

      # @param engine [Engine]
      #
      def initialize(engine)
        @engine = engine
        init_fan_out(engine)
      end


      # PUB is write-only. Engine-facing recv contract: dequeue raises,
      # unblock is a no-op (fatal-error propagation still calls it).
      #
      def recv_queue
        raise "PUB sockets cannot receive"
      end


      def dequeue_recv
        raise "PUB sockets cannot receive"
      end


      def unblock_recv
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        @connections << connection
        @subscriptions[connection] = Set.new
        start_subscription_listener(connection)
        add_fan_out_send_connection(connection)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        @subscriptions.delete(connection)
        remove_fan_out_send_connection(connection)
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        fan_out_enqueue(parts)
      end

    end
  end
end
