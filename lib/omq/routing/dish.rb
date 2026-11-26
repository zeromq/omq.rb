# frozen_string_literal: true

module OMQ
  module Routing
    # DISH socket routing: group-based receive from RADIO peers.
    #
    # Sends JOIN/LEAVE commands to connected RADIO peers.
    # Receives two-frame messages (group + body) from RADIO.
    #
    class Dish
      include ClosableQueues

      # @return [Async::LimitedQueue]
      #
      attr_reader :recv_queue


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine      = engine
        @connections = []
        @recv_queue  = Routing.build_queue(engine.options.recv_hwm, :block)
        @groups      = Set.new
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
        @groups.to_a.each do |group|
          connection.send_command(Protocol::ZMTP::Codec::Command.join(group))
        end
        @engine.start_recv_pump(connection, @recv_queue)
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
      end


      # DISH is read-only.
      #
      def enqueue(_parts)
        raise "DISH sockets cannot send"
      end


      # Joins a group.
      #
      # @param group [String]
      #
      def join(group)
        @groups << group
        @connections.dup.each do |conn|
          conn.send_command(Protocol::ZMTP::Codec::Command.join(group))
        end
      end


      # Leaves a group.
      #
      # @param group [String]
      #
      def leave(group)
        @groups.delete(group)
        @connections.dup.each do |conn|
          conn.send_command(Protocol::ZMTP::Codec::Command.leave(group))
        end
      end

    end
  end
end
