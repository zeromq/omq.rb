# frozen_string_literal: true

module OMQ
  module Routing
    # XSUB socket routing: like SUB but subscriptions sent as data messages.
    #
    # Subscriptions are sent as data frames: \x01 + prefix for subscribe,
    # \x00 + prefix for unsubscribe. Each connected PUB gets its own send
    # queue so subscription commands are delivered independently per peer.
    #
    class XSub
      include ClosableQueues


      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine      = engine
        @connections = Set.new
        @recv_queue  = Routing.build_queue(engine.options.recv_hwm, :block)
        @conn_queues = {}
        @subscriptions = Hash.new(0) # prefix => times subscribed, replayed to new peers
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

        @engine.start_recv_pump(connection, @recv_queue)

        q = Routing.build_queue(@engine.options.send_hwm, :block)
        @conn_queues[connection] = q
        start_conn_send_pump(connection, q, @subscriptions.keys)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        @conn_queues.delete(connection)&.close
      end


      # Enqueues a subscription command (fan-out to all connected PUBs).
      #
      # @param parts [Array<String>]
      #
      # Subscriptions are recorded, like libzmq's XSUB, so a publisher
      # that connects (or reconnects) later gets them too. A cancel goes
      # out only when the last subscription to its prefix is gone.
      #
      def enqueue(parts)
        return unless track_subscription(parts.first)

        @connections.to_a.each do |conn|
          Routing.enqueue_to_peer(@conn_queues[conn], parts)
        end
      end


      # @return [Boolean] true when all per-connection send queues are empty
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end


      private


      # Updates the subscription table for an outgoing message.
      #
      # @param frame [String, nil] first frame: 0x01/0x00 + prefix
      # @return [Boolean] whether to forward the message to the peers
      #
      def track_subscription(frame)
        return true if frame.nil? || frame.empty?

        prefix = frame.byteslice(1..).b

        case frame.getbyte(0)
        when 0x01
          @subscriptions[prefix] += 1
          true
        when 0x00
          count = @subscriptions[prefix]
          return false if count.zero?

          if count > 1
            @subscriptions[prefix] = count - 1
            return false
          end

          @subscriptions.delete(prefix)
          true
        else
          true
        end
      end


      # @param replay [Array<String>] prefixes to subscribe first
      #
      def start_conn_send_pump(conn, q, replay)
        @engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          replay.each do |prefix|
            write_subscription(conn, 0x01, prefix)
          end

          loop do
            parts = q.dequeue or break # queue closed: peer gone
            frame = parts.first&.b

            next if frame.nil? || frame.empty?

            flag = frame.getbyte(0)
            next unless flag == 0x01 || flag == 0x00

            write_subscription(conn, flag, frame.byteslice(1..) || "".b)
          end
        end
      end


      # Writes a SUBSCRIBE (+flag+ 0x01) or CANCEL (0x00) in the form
      # the peer understands: a command for ZMTP 3.1, a message for 3.0.
      #
      def write_subscription(conn, flag, prefix)
        if conn.peer_minor >= 1
          command = flag == 0x01 ? Protocol::ZMTP::Codec::Command.subscribe(prefix) : Protocol::ZMTP::Codec::Command.cancel(prefix)
          conn.send_command(command)
        else
          conn.send_message([flag.chr.b + prefix.b])
        end
      end

    end
  end
end
