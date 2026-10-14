# frozen_string_literal: true

module OMQ
  module Routing
    # REP socket routing: fair-queue receive, reply routed back to sender.
    #
    # REP strips the routing envelope (everything up to and including the
    # empty delimiter) on receive, saves it internally, and restores it
    # on send.
    #
    class Rep
      EMPTY_FRAME = "".b.freeze


      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine        = engine
        @recv_queue    = Routing.build_queue(engine.options.recv_hwm, :block)
        @current_reply = nil
        @receiving     = false
        @conn_queues   = {}
      end


      # Dequeues the next received message. Blocks until one is available.
      #
      # @return [Array<String>, nil]
      #
      def dequeue_recv
        raise SocketError, "REP socket expects recv/send/recv/send order" if @current_reply || @receiving

        @receiving = true
        begin
          request = @recv_queue.dequeue
          return unless request

          connection, envelope, body = request
          @current_reply = [connection, envelope]
          body
        ensure
          @receiving = false
        end
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
        q = Routing.build_queue(@engine.options.send_hwm, :drop_newest)
        @conn_queues[connection] = q

        @engine.start_recv_pump(connection, @recv_queue) do |msg|
          delimiter = msg.index { |p| p.empty? }
          next unless delimiter && delimiter < msg.size - 1

          envelope = msg[0, delimiter].freeze
          body     = msg[(delimiter + 1)..].freeze

          # Keep the routing information with its body until the application
          # receives it, even if the peer disconnects in the meantime.
          @engine.emit_verbose_msg_received(connection, body)
          @recv_queue.enqueue([connection, envelope, body].freeze)
          nil
        end

        ConnSendPump.start(@engine, connection, q)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @conn_queues.delete(connection)
      end


      # Enqueues a reply. Routes to the connection that sent the matching
      # request most recently received by the application. Replies to
      # disconnected or muted peers are discarded.
      #
      # @param parts [Array<String>]
      #
      def enqueue(parts)
        raise SocketError, "REP socket expects recv/send/recv/send order" unless @current_reply

        conn, envelope = @current_reply
        @conn_queues[conn]&.enqueue([*envelope, EMPTY_FRAME, *parts].freeze)
        @current_reply = nil
      end


      # @return [Boolean] true when all per-connection send queues are empty
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end

    end
  end
end
