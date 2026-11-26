# frozen_string_literal: true

module OMQ
  module Routing
    # SUB socket routing: subscription-based receive from PUB peers.
    #
    # Sends SUBSCRIBE/CANCEL commands to connected PUB peers.
    #
    class Sub
      include ClosableQueues


      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine        = engine
        @connections   = Set.new
        @recv_queue    = Routing.build_queue(engine.options.recv_hwm, :block)
        @subscriptions = Set.new
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
        @connections << connection

        @subscriptions.to_a.each do |prefix|
          send_subscribe(connection, prefix)
        end

        @engine.start_recv_pump(connection, @recv_queue)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
      end


      # SUB is read-only.
      #
      def enqueue(_parts)
        raise "SUB sockets cannot send"
      end


      # Subscribes to a topic prefix.
      #
      # @param prefix [String]
      #
      def subscribe(prefix)
        @subscriptions << prefix
        @connections.to_a.each { |conn| send_subscribe(conn, prefix) }
      end


      # Unsubscribes from a topic prefix.
      #
      # @param prefix [String]
      #
      def unsubscribe(prefix)
        @subscriptions.delete(prefix)
        @connections.to_a.each { |conn| send_cancel(conn, prefix) }
      end


      private


      # Sends a SUBSCRIBE to +conn+ using the wire form the peer understands:
      # command-form for ZMTP 3.1+, legacy message-form for ZMTP 3.0.
      #
      def send_subscribe(conn, prefix)
        if conn.peer_minor >= 1
          conn.send_command(Protocol::ZMTP::Codec::Command.subscribe(prefix))
        else
          conn.send_message([Protocol::ZMTP::Codec::Subscription.body(prefix)])
        end
      end


      def send_cancel(conn, prefix)
        if conn.peer_minor >= 1
          conn.send_command(Protocol::ZMTP::Codec::Command.cancel(prefix))
        else
          conn.send_message([Protocol::ZMTP::Codec::Subscription.body(prefix, cancel: true)])
        end
      end

    end
  end
end
