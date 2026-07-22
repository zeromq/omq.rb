# frozen_string_literal: true

module OMQ
  class QoS
    # Prepended onto Protocol::ZMTP::Connection so that command frames
    # received during a normal {Connection#receive_message} loop are
    # dispatched to a per-connection QoS handler. Without this, ACK
    # commands sent by the peer would be silently dropped by the recv
    # pump (which skips command frames).
    #
    module ConnectionExt
      attr_accessor :qos_on_command


      # Re-reads +@qos_on_command+ on each command frame rather than
      # capturing it once at call-start. The PUSH reaper fires
      # +receive_message+ immediately after handshake — before the
      # routing layer has wired up the QoS handler — so a one-time
      # capture would pin the handler to +nil+ for the lifetime of the
      # connection.
      def receive_message(&block)
        super() do |frame|
          if (handler = @qos_on_command)
            cmd = Protocol::ZMTP::Codec::Command.from_body(frame.body)
            handler.call(cmd)
          end
          block&.call(frame)
        end
      end

    end
  end
end


Protocol::ZMTP::Connection.prepend(OMQ::QoS::ConnectionExt)
