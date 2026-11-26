# frozen_string_literal: true

module OMQ
  class Engine
    # Spawns a heartbeat task for a connection.
    #
    # Sends PING frames at +interval+ seconds and closes the connection
    # if no traffic is seen within +timeout+ seconds.
    #
    module Heartbeat
      # @param parent [Async::Task, Async::Barrier] parent to spawn under
      # @param conn [Protocol::ZMTP::Connection]
      # @param options [Options]
      #
      def self.start(parent, conn, options)
        interval = options.heartbeat_interval
        return unless interval&.positive?

        ttl     = options.heartbeat_ttl     || interval
        timeout = options.heartbeat_timeout || interval
        conn.touch_heartbeat

        parent.async(transient: true, annotation: "heartbeat") do
          loop do
            sleep interval
            conn.send_command(Protocol::ZMTP::Codec::Command.ping(ttl: ttl))
            if conn.heartbeat_expired?(timeout)
              conn.close
              break
            end
          end
        rescue Async::Stop, Async::Cancel
        rescue *CONNECTION_LOST
          # connection closed
        end
      end
    end
  end
end
