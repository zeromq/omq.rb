# frozen_string_literal: true

require "timeout"

module OMQ
  # Pure Ruby Readable mixin. Dequeues messages from the engine's recv queue.
  #
  module Readable
    include QueueReadable

    # Receives the next message directly from the engine recv queue.
    #
    # @return [Array<String>] message parts
    # @raise [IO::TimeoutError] if read_timeout exceeded
    #
    def receive
      if @engine.on_io_thread?
        Reactor.run(timeout: @options.read_timeout) { @engine.dequeue_recv }
      elsif (timeout = @options.read_timeout)
        if Reactor.native_fiber_scheduler?
          Async::Task.current.with_timeout(timeout, IO::TimeoutError) { @engine.dequeue_recv }
        else
          Reactor.run(timeout:) { @engine.dequeue_recv }
        end
      else
        @engine.dequeue_recv
      end
    end


    # Waits until the socket is readable.
    #
    # @param timeout [Numeric, nil] timeout in seconds
    # @return [true]
    #
    def wait_readable(timeout = @options.read_timeout)
      true
    end
  end
end
