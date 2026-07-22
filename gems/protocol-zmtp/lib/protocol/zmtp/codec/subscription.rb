# frozen_string_literal: true

module Protocol
  module ZMTP
    module Codec

      # ZMTP subscription encoding.
      #
      # Two wire formats exist and both are in active use:
      #
      # * **Message form (ZMTP 3.0 legacy, RFC 23).** A regular data
      #   frame whose body is `\x01` + prefix (subscribe) or `\x00` +
      #   prefix (cancel). libzmq, JeroMQ, pyzmq, CZMQ, NetMQ all send
      #   subscriptions in this form by default, and all accept it.
      #
      # * **Command form (ZMTP 3.1, RFC 37).** A COMMAND-flagged frame
      #   whose body is a Command named "SUBSCRIBE" or "CANCEL" with
      #   the prefix as the command data.
      #
      # Interop requires sending the message form (understood by every
      # ZMTP 3.0+ peer) and accepting both forms on the receiving side.
      #
      module Subscription
        FLAG_SUBSCRIBE = "\x01".b.freeze
        FLAG_CANCEL    = "\x00".b.freeze

        module_function

        # Builds the body of a subscription message in the legacy
        # message form.
        #
        # @param prefix [String] topic prefix
        # @param cancel [Boolean] true to build an unsubscribe
        # @return [String] binary frame body
        def body(prefix, cancel: false)
          flag = cancel ? FLAG_CANCEL : FLAG_SUBSCRIBE
          flag + (prefix.encoding == Encoding::BINARY ? prefix : prefix.b)
        end


        # Attempts to parse a frame as a subscription. Accepts both the
        # legacy message form and the ZMTP 3.1 command form.
        #
        # @param frame [Frame]
        # @return [Array(Symbol, String), nil] `[:subscribe, prefix]`,
        #   `[:cancel, prefix]`, or `nil` if the frame is not a
        #   subscription
        def parse(frame)
          if frame.command?
            cmd = Command.from_body(frame.body)
            case cmd.name
            when "SUBSCRIBE" then [:subscribe, cmd.data]
            when "CANCEL"    then [:cancel, cmd.data]
            end
          else
            body = frame.body
            return nil if body.empty?

            prefix = body.byteslice(1..) || "".b
            case body.getbyte(0)
            when 0x01 then [:subscribe, prefix]
            when 0x00 then [:cancel, prefix]
            end
          end
        end
      end
    end
  end
end
