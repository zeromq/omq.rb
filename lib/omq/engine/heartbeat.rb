# frozen_string_literal: true

module OMQ
  class Engine
    # ZMTP 3.1 heartbeat for one connection.
    #
    # With +heartbeat_interval+ set, sends a PING every interval. The
    # connection expires when nothing arrived within +heartbeat_timeout+
    # of the oldest PING still unanswered, like libzmq's
    # ZMQ_HEARTBEAT_TIMEOUT: any traffic from the peer counts as an
    # answer.
    #
    # Independently, a peer may put a TTL in its PINGs. The connection
    # then expires when nothing arrives for that long, even without a
    # local heartbeat (libzmq's ZMQ_HEARTBEAT_TTL on the other side).
    #
    class Heartbeat
      # Shortest sleep between checks, so a deadline in the past doesn't
      # spin the loop.
      MIN_SLEEP = 0.001


      # @param parent [Async::Task, Async::Barrier] parent to spawn under
      # @param conn [Protocol::ZMTP::Connection]
      # @param options [Options]
      # @return [Async::Task, nil]
      #
      def self.start(parent, conn, options)
        # ZMTP 3.0 has no PING/PONG; a 3.0 peer would see a bad command.
        return if conn.respond_to?(:peer_minor) && conn.peer_minor.to_i < 1

        interval = options.heartbeat_interval
        interval = nil unless interval&.positive?
        timeout  = options.heartbeat_timeout
        timeout  = interval unless timeout&.positive?

        conn.touch_heartbeat
        heartbeat = new(conn, interval: interval, ttl: advertised_ttl(options), timeout: timeout)

        if interval
          heartbeat.spawn(parent)
        elsif conn.respond_to?(:on_peer_ttl=)
          # No local heartbeat: watch only once the peer asks for it.
          conn.on_peer_ttl = -> { heartbeat.spawn(parent) }
          nil
        end
      end


      # TTL to put in our PINGs. Like libzmq and omq.rs, none unless
      # +heartbeat_ttl+ is set: a TTL equal to the interval would make
      # peers that honor it drop us whenever a PING runs late.
      #
      # @param options [Options]
      # @return [Numeric] seconds, 0 = no TTL
      #
      def self.advertised_ttl(options)
        options.heartbeat_ttl || 0
      end


      # @param conn [Protocol::ZMTP::Connection]
      # @param interval [Numeric, nil] seconds between PINGs, nil = don't send
      # @param ttl [Numeric] TTL advertised in each PING
      # @param timeout [Numeric, nil] seconds a PING may stay unanswered
      #
      def initialize(conn, interval:, ttl:, timeout:)
        @conn      = conn
        @interval  = interval
        @ping      = Protocol::ZMTP::Codec::Command.ping(ttl: ttl)
        @timeout   = timeout
        @pending   = nil   # monotonic time of the oldest unanswered PING
        @sending   = false # a PING is still waiting to be written
        @next_ping = interval && Async::Clock.now + interval
      end


      # @param parent [Async::Task, Async::Barrier]
      # @return [Async::Task]
      #
      def spawn(parent)
        parent.async(transient: true, annotation: "heartbeat") do
          run
        rescue Async::Stop, Async::Cancel
        rescue *CONNECTION_LOST
          # connection closed
        end
      end


      # Runs until the connection expires (and is closed) or the task
      # is stopped.
      #
      # @return [void]
      #
      def run
        loop do
          sleep wake_in(Async::Clock.now)

          now = Async::Clock.now

          if expired?(now)
            @conn.close
            break
          end

          ping(now) if @next_ping && now >= @next_ping
        end
      end


      private


      def wake_in(now)
        wake = [@next_ping, ttl_deadline].compact.min
        [wake - now, MIN_SLEEP].max
      end


      # @return [Float, nil] when the peer's TTL runs out without traffic
      #
      def ttl_deadline
        ttl  = @conn.respond_to?(:peer_ttl) && @conn.peer_ttl
        last = @conn.last_received_at
        return unless ttl && last

        last + ttl
      end


      def expired?(now)
        # We stopped reading (full receive queue), not the peer.
        return false if @conn.respond_to?(:reading_paused?) && @conn.reading_paused?

        deadline = ttl_deadline
        return true if deadline && now >= deadline

        pending_expired?(now)
      end


      def pending_expired?(now)
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
        @next_ping = now + @interval
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
