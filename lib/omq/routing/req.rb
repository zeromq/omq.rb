# frozen_string_literal: true

module OMQ
  module Routing
    # REQ socket routing: round-robin send with strict send/recv alternation.
    #
    # REQ prepends an empty delimiter frame on send and strips it on receive.
    #
    class Req
      include RoundRobin

      # Shared frozen empty binary string to avoid repeated allocations.
      EMPTY_BINARY = ::Protocol::ZMTP::Codec::EMPTY_BINARY


      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine           = engine
        @recv_queue       = Routing.build_queue(engine.options.recv_hwm, :block)
        @state            = :ready
        @reply_connection = nil
        @receiving        = false
        init_round_robin(engine)
      end


      # Dequeues the next received message. Blocks until one is available.
      #
      # @return [Array<String>, nil]
      #
      def dequeue_recv
        raise SocketError, "REQ socket expects send/recv/send/recv order" if @state == :ready || @receiving

        @receiving = true
        begin
          msg = @recv_queue.dequeue
          if msg
            @state            = :ready
            @reply_connection = nil
          end
          msg
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
        @engine.start_recv_pump(connection, @recv_queue) do |msg|
          next unless @state == :waiting_reply && connection.equal?(@reply_connection)
          next unless msg.size >= 2 && msg.first.empty?

          @state = :reply_queued
          msg[1..].freeze
        end

        add_round_robin_send_connection(connection)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        remove_round_robin_send_connection(connection)
      end


      # @param parts [Array<String>]
      #
      def enqueue(parts)
        raise SocketError, "REQ socket expects send/recv/send/recv order" unless @state == :ready
        @state = :waiting_reply
        accepted = false
        begin
          @reply_connection = @direct_pipe if @direct_pipe&.direct_recv_queue
          enqueue_round_robin(parts)
          accepted = true
        ensure
          unless accepted
            @state            = :ready
            @reply_connection = nil
          end
        end
      end


      private


      # The send pump selects the peer when it takes the request off the queue.
      def write_batch(connection, batch)
        @reply_connection = connection
        super
      end


      # REQ prepends empty delimiter frame on the wire.
      #
      def transform_send(parts)
        parts.dup.unshift(EMPTY_BINARY).freeze
      end

    end
  end
end
