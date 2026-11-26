# frozen_string_literal: true

module OMQ
  module Routing
    # XPUB socket routing: like PUB but exposes subscription messages.
    #
    # Subscription/unsubscription messages from peers are delivered to
    # the application as data frames: \x01 + prefix for subscribe,
    # \x00 + prefix for unsubscribe.
    #
    # The recv_queue is a simple bounded queue because messages come from
    # subscription commands, not from peer data pumps.
    #
    class XPub
      include FanOut
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine     = engine
        @recv_queue = Routing.build_queue(engine.options.recv_hwm, :block)

        init_fan_out(engine)
      end


      # Engine-facing recv contract. Delegates to the bounded queue.
      #
      def dequeue_recv
        @recv_queue.dequeue
      end


      def unblock_recv
        @recv_queue.push(nil)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        @connections << connection
        @subscriptions[connection] = PrefixSet.new
        start_subscription_listener(connection)
        add_fan_out_send_connection(connection)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        prefixes = @subscriptions.delete(connection)
        remove_fan_out_send_connection(connection)
        report_lost_subscriptions(prefixes)
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        fan_out_enqueue(parts)
      end


      private


      # Expose subscription to application as data message.
      #
      def on_subscribe(conn, prefix)
        super
        @recv_queue.enqueue(["\x01#{prefix}".b])
      end


      # Expose unsubscription to application as data message.
      #
      # A lost subscriber's subscriptions end with it. Report a cancel
      # per prefix, as libzmq does, so a forwarder can drop them
      # upstream. Queued from its own task: teardown must not block on a
      # full receive queue.
      #
      # @param prefixes [PrefixSet, nil]
      #
      def report_lost_subscriptions(prefixes)
        return if prefixes.nil? || prefixes.empty?
        return if @engine.closed?

        cancels = prefixes.map { |prefix| ["\x00#{prefix}".b] }

        @engine.barrier.async(transient: true, annotation: "xpub lost subscriptions") do
          cancels.each { |msg| @recv_queue.enqueue(msg) }
        rescue Async::Queue::ClosedError
          # socket closed meanwhile
        end
      end


      def on_cancel(conn, prefix)
        super
        @recv_queue.enqueue(["\x00#{prefix}".b])
      end

    end
  end
end
