# frozen_string_literal: true

require "async"
require "async/queue"
require "async/limited_queue"
require_relative "drop_queue"
require_relative "routing/conn_send_pump"
require_relative "routing/closable_queues"
require_relative "routing/prefix_set"

module OMQ
  # Routing strategies for each ZMQ socket type.
  #
  # Each strategy manages how messages flow between connections and
  # the socket's send/recv queues.
  #
  module Routing
    # Raised by #connection_added when a strategy can't take another
    # peer (PAIR, CHANNEL). The connection is closed and, if this side
    # dialed, redialed later.
    #
    class PeerRefusedError < RuntimeError
    end


    # Errors an enqueue raises once the queue was closed: the peer it
    # belonged to is gone, or the socket closed.
    QUEUE_CLOSED_ERRORS = [Async::Queue::ClosedError, ClosedQueueError].freeze


    # Plugin registry for socket types not built into omq.
    # Populated by sister gems via +Routing.register+.
    #
    @registry = {}


    class << self
      # @return [Hash{Symbol => Class}] plugin registry
      attr_reader :registry


      # Registers a routing strategy class for a socket type.
      # Called by omq-draft (and other plugins) at require time.
      #
      # @param socket_type [Symbol] e.g. :RADIO, :CLIENT
      # @param strategy_class [Class]
      #
      def register(socket_type, strategy_class)
        @registry[socket_type] = strategy_class
      end
    end


    # Refuses one more subscription prefix or group from a peer that
    # already holds +limit+. The raised error drops the peer.
    #
    # @param held [Set<String>] what the peer holds now
    # @param item [String] the prefix or group it asks for
    # @param limit [Integer, nil] max_peer_subscriptions
    # @raise [Protocol::ZMTP::Error]
    #
    def self.check_peer_subscriptions!(held, item, limit)
      return unless limit
      return if held.size < limit || held.include?(item)

      raise Protocol::ZMTP::Error, "peer exceeds max_peer_subscriptions #{limit}"
    end


    # Builds a send or recv queue based on the mute strategy.
    #
    # @param hwm [Integer] high water mark
    # @param on_mute [Symbol] :block, :drop_newest, or :drop_oldest
    # @return [Async::LimitedQueue, DropQueue]
    #
    def self.build_queue(hwm, on_mute)
      return Async::Queue.new if hwm.nil? || hwm == 0

      case on_mute
      when :block
        Async::LimitedQueue.new(hwm)
      when :drop_newest, :drop_oldest
        DropQueue.new(hwm, strategy: on_mute)
      else
        raise ArgumentError, "unknown on_mute strategy: #{on_mute.inspect}"
      end
    end


    # Drains all available messages from +queue+ into +batch+ without
    # Blocks for the first message, then sweeps all immediately
    # available messages into +batch+ without blocking.
    #
    # No cap is needed: IO::Stream auto-flushes at 64 KB, so the
    # write buffer hits the wire naturally under sustained load.
    # The explicit flush after the batch pushes out the remainder.
    #
    # @param queue [Async::LimitedQueue]
    # @param batch [Array]
    # @return [Array, nil] +batch+, or nil once +queue+ is closed and
    #   empty (the pump should stop)
    #
    def self.dequeue_batch(queue, batch = [])
      first = queue.dequeue or return
      batch << first

      loop do
        msg = queue.dequeue(timeout: 0) or break
        batch << msg
      end

      batch
    end


    # Enqueues into a per-connection queue, dropping the message if the
    # peer left while the caller was blocked at HWM.
    #
    # @param queue [Async::LimitedQueue, DropQueue, nil]
    # @param parts [Array<String>]
    # @return [void]
    #
    def self.enqueue_to_peer(queue, parts)
      queue&.enqueue(parts)
    rescue *QUEUE_CLOSED_ERRORS
      # peer disconnected; its queue is gone
    end


    # Returns the routing strategy class for a socket type.
    #
    # @param socket_type [Symbol] e.g. :PAIR, :REQ
    # @return [Class]
    #
    def self.for(socket_type)
      case socket_type
      when :PAIR
        Pair
      when :REQ
        Req
      when :REP
        Rep
      when :DEALER
        Dealer
      when :ROUTER
        Router
      when :PUB
        Pub
      when :SUB
        Sub
      when :XPUB
        XPub
      when :XSUB
        XSub
      when :PUSH
        Push
      when :PULL
        Pull
      else
        @registry[socket_type] or raise ArgumentError, "unknown socket type: #{socket_type.inspect}"
      end
    end

  end
end
