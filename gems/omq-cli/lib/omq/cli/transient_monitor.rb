# frozen_string_literal: true

module OMQ
  module CLI
    # Monitors peer-disconnect events for --transient mode.
    #
    # Starts an async task that waits until #ready! is called (signalling
    # that at least one message has been exchanged), then waits for all
    # peers to disconnect, disables reconnection, and either stops the
    # task (send-only) or closes the read side of the socket (recv side).
    #
    class TransientMonitor
      # @param sock [Object] the OMQ socket to monitor
      # @param config [Config] frozen CLI configuration
      # @param task [Async::Task] parent async task
      # @param log_fn [Method] callable for verbose logging
      def initialize(sock, config, task, log_fn)
        @barrier = Async::Promise.new
        task.async do
          @barrier.wait
          sock.all_peers_gone.wait unless sock.connection_count == 0
          log_fn.call("all peers disconnected, exiting")
          sock.reconnect_enabled = false
          if config.send_only?
            task.stop
          else
            sock.close_read
          end
        end
      end


      # Signal that the first message has been sent or received.
      # Idempotent -- safe to call multiple times.
      #
      def ready!
        @barrier.resolve(true) unless @barrier.resolved?
      end
    end
  end
end
