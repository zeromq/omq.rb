# frozen_string_literal: true

require "async"

module OMQ
  # Shared IO reactor for the Ruby backend.
  #
  # When user code runs inside an Async reactor, engine tasks are
  # spawned directly under the caller's Async task. When no reactor
  # is available (e.g. bare Thread.new), a single shared IO thread
  # hosts all engine tasks — mirroring libzmq's IO thread.
  #
  # Engines obtain the IO thread's root task via {.root_task} and
  # use it as their @parent_task. Blocking operations from the main
  # thread are dispatched to the IO thread via {.run}.
  #
  module Reactor
    THREAD_NAME = 'omq-io'

    @mutex      = Mutex.new
    @pid        = nil
    @thread     = nil
    @root_task  = nil
    @work_queue = nil
    @lingers    = Hash.new(0) # linger value → count of active sockets


    class << self
      # @return [Hash{Numeric => Integer}] linger value → active socket count
      #
      attr_reader :lingers


      # Returns the root Async task inside the shared IO thread.
      # Starts the thread exactly once (double-checked lock).
      #
      # @return [Async::Task]
      #
      def root_task
        pid = Process.pid
        return @root_task if @root_task && @pid == pid

        reset_after_fork if @pid && @pid != pid

        @mutex.synchronize do
          return @root_task if @root_task && @pid == pid

          ready        = Thread::Queue.new
          @work_queue  = Async::Queue.new
          @thread      = Thread.new { run_reactor(ready) }
          @thread.name = THREAD_NAME
          @root_task   = ready.pop
          @pid         = pid

          at_exit { stop! }
        end

        @root_task
      end


      # Runs a block inside the Async reactor.
      #
      # Inside an Async reactor: runs directly.
      # Outside: dispatches to the shared IO thread and blocks
      # the calling thread until the result is available.
      #
      # @return [Object] the block's return value
      #
      def run(timeout: nil, &block)
        task = Async::Task.current?

        if task
          if timeout
            task.with_timeout(timeout, IO::TimeoutError) { yield }
          else
            yield
          end
        else
          result = Async::Promise.new
          root_task # ensure started
          @work_queue << [block, result, timeout]
          result.wait
        end
      end


      # Registers a socket's linger value.
      #
      # @param seconds [Numeric, nil] linger value
      #
      def track_linger(seconds)
        @lingers[seconds || 0] += 1
      end


      # Unregisters a socket's linger value.
      #
      # @param seconds [Numeric, nil] linger value
      #
      def untrack_linger(seconds)
        key            = seconds || 0
        @lingers[key] -= 1

        if @lingers[key] <= 0
          @lingers.delete(key)
        end
      end


      # Stops the shared IO thread.
      #
      # @return [void]
      #
      def stop!
        if @pid && @pid != Process.pid
          reset_after_fork
          return
        end

        return unless @thread&.alive?

        max_linger = @lingers.empty? ? 0 : @lingers.keys.max

        @work_queue << nil if @work_queue
        @thread&.join(max_linger + 1)

        @thread     = nil
        @root_task  = nil
        @work_queue = nil
        @pid        = nil
        @lingers.clear
      end


      private


      def reset_after_fork
        @mutex      = Mutex.new
        @thread     = nil
        @root_task  = nil
        @work_queue = nil
        @pid        = nil
        @lingers.clear
      end


      # Runs the shared Async reactor.
      #
      # Processes work items dispatched via {.run} while engine
      # tasks (accept loops, pumps, etc.) run as transient children.
      #
      # @param ready [Thread::Queue] receives the root task once started
      #
      def run_reactor(ready)
        Async do |task|
          ready.push(task)

          loop do
            item = @work_queue.dequeue or break
            block, result, timeout = item

            task.async do |t|
              if timeout
                result.fulfill do
                  t.with_timeout(timeout, IO::TimeoutError) { block.call }
                end
              else
                result.fulfill { block.call }
              end
            end
          end
        end
      end

    end
  end
end
