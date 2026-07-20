# frozen_string_literal: true

require "protocol/zmtp"

module OMQ
  module Transport
    module WebSocket
      # ZWS 2.0 wire codec (RFC 45). One ZeroMQ frame per WebSocket
      # binary message, prefixed with a single FLAG byte:
      #
      #   0x00  final data frame   (no MORE)
      #   0x01  intermediate data frame (MORE)
      #   0x02  command frame      (READY/PING/PONG/SUBSCRIBE/...)
      #
      # No length prefix — the WebSocket message length IS the frame
      # length. No greeting; mechanism is negotiated via
      # Sec-WebSocket-Protocol during the HTTP upgrade.
      module Codec

        FLAG_LAST    = 0x00
        FLAG_MORE    = 0x01
        FLAG_COMMAND = 0x02

        EMPTY_BINARY = "".b.freeze


        # Encodes a frame body with the appropriate FLAG byte prefix.
        #
        # @param body [String] frame payload (binary)
        # @param more [Boolean] more frames follow in this message
        # @param command [Boolean] command frame (PING/PONG/READY/...)
        # @return [String] WS binary message bytes (BINARY encoding)
        #
        def self.encode(body, more: false, command: false)
          flag    = command ? FLAG_COMMAND : (more ? FLAG_MORE : FLAG_LAST)
          payload = body.encoding == Encoding::BINARY ? body : body.b
          out     = String.new(capacity: payload.bytesize + 1, encoding: Encoding::BINARY)

          out << flag.chr(Encoding::BINARY)
          out << payload
          out
        end


        # Decodes a WS binary message into [flag_byte, body_slice].
        #
        # @param bytes [String] WS binary message bytes
        # @return [Array(Integer, String)]
        # @raise [Protocol::ZMTP::Error] on empty message
        #
        def self.decode(bytes)
          raise Protocol::ZMTP::Error, "ZWS frame is empty" if bytes.empty?

          flag = bytes.getbyte(0)
          body = bytes.byteslice(1, bytes.bytesize - 1) || EMPTY_BINARY
          [flag, body]
        end

      end
    end
  end
end
