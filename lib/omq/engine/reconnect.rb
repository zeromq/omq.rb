# frozen_string_literal: true

module OMQ
  class Engine
    # Schedules reconnect attempts with exponential back-off.
    #
    # Runs a background task that loops until a connection is established
    # or the engine is closed.
    #
    class Reconnect
      # @param dialer [Transport::TCP::Dialer, etc.] stateful dialer factory
      # @param options [Options]
      # @param parent_task [Async::Task]
      # @param engine [Engine]
      # @param delay [Numeric, nil] initial delay (defaults to reconnect_interval)
      #
      def self.schedule(dialer, options, parent_task, engine, delay: nil)
        new(engine, dialer, options).run(parent_task, delay: delay)
      end


      # @param engine [Engine]
      # @param dialer [Transport::TCP::Dialer, etc.] stateful dialer factory
      # @param options [Options]
      #
      def initialize(engine, dialer, options)
        @engine  = engine
        @dialer  = dialer
        @options = options
      end


      # Spawns a background task that retries the connection with exponential backoff.
      #
      # @param parent_task [Async::Task]
      # @param delay [Numeric, nil] initial delay override
      # @return [void]
      #
      def run(parent_task, delay: nil)
        endpoint = @dialer.endpoint
        parent_task.async(transient: true, annotation: "reconnect #{endpoint}") do
          retry_loop(delay: delay)
        rescue Async::Stop
        rescue => error
          @engine.signal_fatal_error(error)
        end
      end


      private


      def retry_loop(delay: nil)
        delay, max_delay = init_delay(delay)

        loop do
          break if @engine.closed?
          sleep quantized_wait(delay) if delay > 0
          break if @engine.closed?
          begin
            @dialer.connect
            break
          rescue Protocol::ZMTP::FatalHandshakeError
            break
          rescue *CONNECTION_LOST, *CONNECTION_FAILED, Protocol::ZMTP::Error
            delay = next_delay(delay, max_delay)
            @engine.emit_monitor_event :connect_retried,
                                       endpoint: @dialer.endpoint, detail: { interval: delay }
          end
        end
      end


      # Wall-clock quantized sleep: wait until the next +delay+-sized
      # grid tick. Multiple clients reconnecting with the same interval
      # wake up at the same instant, collapsing staggered retries into
      # aligned waves. Same math as +Async::Loop.quantized+.
      #
      # Wall-clock (not monotonic) on purpose: the grid has to line up
      # across processes, and monotonic clocks don't share an origin.
      # Anti-jitter by design — if you want spread, don't call this.
      #
      # @param delay [Numeric] grid interval in seconds
      # @param now [Float] wall-clock time in seconds (injectable for tests)
      # @return [Float] seconds to sleep, always in (0, delay]
      #
      def quantized_wait(delay, now = Time.now.to_f)
        wait = delay - (now % delay)
        wait.positive? ? wait : delay
      end


      def init_delay(delay)
        ri = @options.reconnect_interval
        if ri.is_a?(Range)
          [delay || ri.begin, ri.end]
        else
          [delay || ri, nil]
        end
      end


      def next_delay(delay, max_delay)
        ri = @options.reconnect_interval
        if ri.is_a?(Range)
          delay = delay * 2
          delay = [delay, max_delay].min if max_delay
          delay = ri.begin if delay == 0
          delay
        else
          ri
        end
      end

    end
  end
end
