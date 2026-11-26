# frozen_string_literal: true

require "async/notification"

module OMQ
  # Event queue behind {Socket#monitor}.
  #
  # Lifecycle events never block the engine fiber that emits them, so a
  # slow monitor block can't stall accept loops, handshakes or teardown.
  # Verbose message events (opt-in tracing) wait while
  # {VERBOSE_BACKLOG} events are pending: tracing a busy socket applies
  # backpressure instead of growing without bound.
  #
  class MonitorQueue
    # Pending events above which verbose emitters wait.
    VERBOSE_BACKLOG = 64


    def initialize
      @queue   = Thread::Queue.new
      @drained = Async::Notification.new
    end


    # Appends a lifecycle event (or the +nil+ end-of-stream marker).
    # Never blocks.
    #
    # @param event [MonitorEvent, nil]
    # @return [void]
    #
    def push(event)
      @queue.push(event)
    rescue ClosedQueueError
      # monitor stopped
    end

    alias << push
    alias enqueue push


    # Appends a verbose event, waiting while the consumer is behind.
    #
    # @param event [MonitorEvent]
    # @return [void]
    #
    def push_verbose(event)
      while @queue.size >= VERBOSE_BACKLOG && !@queue.closed?
        @drained.wait
      end

      push(event)
    end


    # @return [MonitorEvent, nil] the next event; nil marks the end
    #
    def dequeue
      event = @queue.pop
      @drained.signal
      event
    end


    # Stops accepting events and releases waiting verbose emitters.
    #
    # @return [void]
    #
    def close
      @queue.close
      @drained.signal
    end

  end
end
