# frozen_string_literal: true

module OMQ
  class QoS
    # NACK error codes and exception mapping for QoS 3.
    #
    # Receiver side: applications raise one of these exceptions from the
    # block passed to +#receive+; the mapped error code travels back to
    # the sender in a NACK command.
    #
    # Sender side: NACK payloads are decoded into {NackInfo} (attached to
    # {DeadLetter#error} when the NACK is terminal or retries are
    # exhausted).
    #
    # The high bit (+0x80+) of the error code byte is the *retryable*
    # flag (see RFC §163–196). Senders MUST respect this flag for unknown
    # codes.
    #
    module ErrorCodes
      CODE_TIMEOUT    = 0x81  # retryable
      CODE_BAD_INPUT  = 0x02  # terminal
      CODE_INTERNAL   = 0x83  # retryable
      CODE_OVERLOADED = 0x84  # retryable
      CODE_REJECTED   = 0x05  # terminal

      RETRYABLE_BIT = 0x80
      MAX_MSG_BYTES = 65_535


      # @param code [Integer]
      # @return [Boolean]
      def self.retryable?(code)
        (code & RETRYABLE_BIT) != 0
      end


      # Maps an exception to a NACK code + truncated UTF-8 message
      # suitable for the wire.
      #
      # @param exc [Exception]
      # @return [Array(Integer, String)] [code, message bytes]
      def self.exception_to_payload(exc)
        code =
          case exc
          when TimeoutError    then CODE_TIMEOUT
          when BadInputError   then CODE_BAD_INPUT
          when OverloadedError then CODE_OVERLOADED
          when RejectedError   then CODE_REJECTED
          else                      CODE_INTERNAL
          end

        msg = (exc.message || "").b
        msg = msg.byteslice(0, MAX_MSG_BYTES) if msg.bytesize > MAX_MSG_BYTES
        [code, msg]
      end
    end


    # Raised by a QoS 3 receive block when processing exceeds
    # +processing_timeout+. Retryable (code +0x81+).
    class TimeoutError < StandardError
    end


    # Raised by the application to indicate malformed or invalid input.
    # Terminal (code +0x02+).
    class BadInputError < StandardError
    end


    # Raised by the application to indicate capacity pressure. Retryable
    # (code +0x84+).
    class OverloadedError < StandardError
    end


    # Raised by the application for explicit rejection. Terminal
    # (code +0x05+).
    class RejectedError < StandardError
    end


    # Raised to a QoS 3 REQ sender's fiber when the REP peer returns a
    # NACK. Carries the wire-level code + message so the caller can
    # inspect them.
    class NackError < StandardError
      attr_reader :code


      def initialize(code, message)
        super(message)
        @code = code
      end

    end


    # Decoded NACK payload. Attached to {DeadLetter#error} at QoS 3 when
    # the failure is terminal or retries are exhausted.
    NackInfo = Data.define(:code, :message)
  end
end
