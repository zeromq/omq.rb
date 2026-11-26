# frozen_string_literal: true

module OMQ
  module Transport
    # Error handling shared by stream listener accept loops (TCP, IPC).
    #
    # A listener must survive errors that a later accept(2) recovers
    # from. Without this, one EMFILE ended the accept task for good and
    # left the endpoint bound but deaf.
    #
    module AcceptLoop
      # accept(2) ran out of a resource. Retry after a pause so a full fd
      # table doesn't spin the loop.
      BACKOFF_ERRORS = [
        Errno::EMFILE,
        Errno::ENFILE,
        Errno::ENOBUFS,
        Errno::ENOMEM,
      ].freeze


      # One client failed during accept(2) or socket setup. Drop it and
      # accept the next one right away.
      CLIENT_ERRORS = %i[ECONNABORTED ECONNRESET EPROTO EPERM EINVAL ENOTCONN].filter_map do |name|
        Errno.const_get(name) if Errno.const_defined?(name)
      end.freeze


      # Every error an accept loop recovers from.
      RECOVERABLE_ERRORS = (BACKOFF_ERRORS + CLIENT_ERRORS).freeze


      # Reports a recoverable accept error and backs off if the process
      # is out of a resource.
      #
      # @param error [SystemCallError]
      # @param engine [Engine]
      # @param endpoint [String]
      # @return [void]
      #
      def self.recover(error, engine, endpoint)
        engine.emit_monitor_event(:accept_failed, endpoint: endpoint, detail: { error: error })

        if BACKOFF_ERRORS.include?(error.class)
          sleep backoff(engine.options)
        end
      end


      # @param options [Options]
      # @return [Numeric] the shortest reconnect interval
      #
      def self.backoff(options)
        ri = options.reconnect_interval
        ri.is_a?(Range) ? ri.begin : ri
      end

    end
  end
end
