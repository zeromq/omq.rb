# frozen_string_literal: true

module OMQ
  # A bounded queue that drops messages when full instead of blocking.
  #
  # Two drop strategies:
  #   :drop_newest — discard the incoming message (tail drop)
  #   :drop_oldest — discard the head, then enqueue the new message
  #
  # Used by SUB/XSUB/DISH recv queues when on_mute is a drop strategy.
  #
  class DropQueue
    # @param limit [Integer] maximum number of items
    # @param strategy [Symbol] :drop_newest or :drop_oldest
    #
    def initialize(limit, strategy: :drop_newest)
      @queue    = Thread::SizedQueue.new(limit)
      @strategy = strategy
    end


    # Enqueues an item. Drops according to the configured strategy if full.
    #
    # @param item [Object]
    # @return [void]
    #
    def enqueue(item)
      @queue.push(item, true)
    rescue ThreadError
      return if @strategy == :drop_newest

      # :drop_oldest — discard head, enqueue new
      @queue.pop(true) rescue nil
      retry
    end


    # Removes and returns the next item, blocking if empty.
    #
    # @return [Object]
    #
    def dequeue(timeout: nil)
      if timeout
        @queue.pop(timeout: timeout)
      else
        @queue.pop
      end
    end


    # Closes the queue. Blocked and later dequeues return +nil+ once it
    # is empty; enqueues raise +ClosedQueueError+.
    #
    # @return [void]
    #
    def close
      @queue.close
    end


    # @return [Boolean]
    #
    def closed?
      @queue.closed?
    end


    # @return [Boolean]
    #
    def empty?
      @queue.empty?
    end
  end
end
