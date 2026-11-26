# frozen_string_literal: true

module OMQ
  class Engine
    # Monitor events. Uses the engine's +@monitor_queue+ and
    # +@verbose_monitor+.
    #
    module Monitoring
      # Emits a lifecycle event to the monitor queue, if one is attached.
      #
      # @param type [Symbol] event type (e.g. :listening, :connected, :disconnected)
      # @param endpoint [String, nil] the endpoint involved
      # @param detail [Hash, nil] extra context
      # @return [void]
      #
      def emit_monitor_event(type, endpoint: nil, detail: nil)
        return unless @monitor_queue

        @monitor_queue << MonitorEvent.new(type: type, endpoint: endpoint, detail: detail)
      end


      # Emits a verbose-only monitor event (e.g. message traces).
      # Only emitted when {Socket#monitor} was called with +verbose: true+.
      # Uses +**detail+ to avoid Hash allocation when verbose is off.
      #
      # @param type [Symbol] event type (e.g. :message_sent, :message_received)
      # @param detail [Hash] extra context forwarded as keyword args
      # @return [void]
      #
      def emit_verbose_monitor_event(type, **detail)
        return unless @verbose_monitor

        push_verbose_event(type, detail)
      end


      # Emits a :message_sent verbose event and enriches it with the
      # on-wire (post-compression) byte size if +conn+ exposes
      # +last_wire_size_out+ (installed by ZMTP-Zstd etc.).
      def emit_verbose_msg_sent(conn, parts)
        return unless @verbose_monitor

        detail = { parts: parts }

        if conn.respond_to? :last_wire_size_out
          detail[:wire_size] = conn.last_wire_size_out
        end

        push_verbose_event(:message_sent, detail)
      end


      # Emits a :message_received verbose event and enriches it with the
      # on-wire (pre-decompression) byte size if +conn+ exposes
      # +last_wire_size_in+.
      def emit_verbose_msg_received(conn, parts)
        return unless @verbose_monitor

        detail = { parts: parts }

        if conn.respond_to? :last_wire_size_in
          detail[:wire_size] = conn.last_wire_size_in
        end

        push_verbose_event(:message_received, detail)
      end


      private


      # Verbose events wait for the monitor to catch up (see
      # {MonitorQueue#push_verbose}).
      #
      def push_verbose_event(type, detail)
        queue = @monitor_queue or return

        queue.push_verbose(MonitorEvent.new(type: type, detail: detail))
      rescue Async::Stop
      end


      # Signals end-of-stream on the monitor queue (if any) by pushing a
      # +nil+ sentinel, so consumers iterating with {Socket#each_event}
      # can exit cleanly.
      #
      def close_monitor_queue
        return unless @monitor_queue
        @monitor_queue.push(nil)
      end

    end
  end
end
