# frozen_string_literal: true

module OMQ
  class Engine
    # ZMTP 3.1 heartbeat for one connection.
    #
    # Sends a PING every +interval+ seconds. The connection expires when
    # nothing arrived within +timeout+ seconds of the oldest PING still
    # unanswered, like libzmq's ZMQ_HEARTBEAT_TIMEOUT: any traffic from
    # the peer counts as an answer.
    #
    class Heartbeat
      # @param parent [Async::Task, Async::Barrier] parent to spawn under
      # @param conn [Protocol::ZMTP::Connection]
      # @param options [Options]
      # @return [Async::Task, nil]
      #
      def self.start(parent, conn, options)
        interval = options.heartbeat_interval
        return unless interval&.positive?

        ttl     = options.heartbeat_ttl || interval
        timeout = options.heartbeat_timeout
        timeout = interval unless timeout&.positive?

        conn.touch_heartbeat
        heartbeat = new(conn, interval: interval, ttl: ttl, timeout: timeout)

        parent.async(transient: true, annotation: "heartbeat") do
          heartbeat.run
        rescue Async::Stop, Async::Cancel
        rescue *CONNECTION_LOST
          # connection closed
        end
      end


      # @param conn [Protocol::ZMTP::Connection]
      # @param interval [Numeric] seconds between PINGs
      # @param ttl [Numeric] TTL advertised in each PING
      # @param timeout [Numeric] seconds a PING may stay unanswered
      #
      def initialize(conn, interval:, ttl:, timeout:)
        @conn     = conn
        @interval = interval
        @ping     = Protocol::ZMTP::Codec::Command.ping(ttl: ttl)
        @timeout  = timeout
        @pending  = nil   # monotonic time of the oldest unanswered PING
        @sending  = false # a PING is still waiting to be written
      end


      # Runs until the connection expires (and is closed) or the task
      # is stopped.
      #
      # @return [void]
      #
      def run
        loop do
          sleep @interval

          now = Async::Clock.now

          if expired?(now)
            @conn.close
            break
          end

          ping(now)
        end
      end


      private


      def expired?(now)
        return false unless @pending

        last = @conn.last_received_at

        if last && last >= @pending
          @pending = nil
          return false
        end

        now - @pending >= @timeout
      end


      # Sends the PING from a child fiber of the heartbeat task: the
      # write may wait behind a send that is stuck on a peer that
      # stopped reading, and the expiry check must keep running
      # meanwhile. (Not on the connection barrier, where a finished
      # task means a dead pump.)
      #
      def ping(now)
        @pending ||= now
        return if @sending

        @sending = true

        Async::Task.current.async(transient: true, annotation: "heartbeat ping") do
          @conn.send_command(@ping)
        rescue Async::Stop, Async::Cancel, *CONNECTION_LOST
          # connection closing
        ensure
          @sending = false
        end
      end

    end
  end
end
