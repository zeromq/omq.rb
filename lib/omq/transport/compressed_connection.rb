# frozen_string_literal: true

require "delegate"
require_relative "compression_arena"

module OMQ
  module Transport
    class CompressedConnection < SimpleDelegator
      attr_reader :last_wire_size_in

      def initialize(conn, max_message_size:, arena_sentinel:, protocol_error:)
        super(conn)
        @max_message_size   = max_message_size
        @arena_sentinel    = arena_sentinel
        @protocol_error    = protocol_error
        @pending_messages  = []
        @last_wire_size_in = nil

        # The outer framer sees transport envelopes, not logical messages.
        # Also allow LZ4's worst-case block overhead and dictionary shipment.
        if max_message_size && conn.respond_to?(:max_message_size=)
          overhead = max_message_size / 255 + 32 + 20 * (max_message_size / 1_073_741_824 + 1)
          conn.max_message_size = [max_message_size + overhead, CompressionArena::MAX_WIRE_SIZE].max
        end
      end


      def receive_message(&block)
        unless @pending_messages.empty?
          @last_wire_size_in = 0
          return @pending_messages.shift
        end

        loop do
          parts = __getobj__.receive_message(&block)
          if parts.any? { |part| part.start_with?(@arena_sentinel) }
            raise @protocol_error, "arena must be a single-part message" unless parts.size == 1

            @pending_messages = CompressionArena.decode(
              parts.first, max_message_size: @max_message_size, error: @protocol_error,
            ) { |data, size| decompress_arena(data, size) }
            @last_wire_size_in = parts.first.bytesize
            return @pending_messages.shift
          end

          decoded = decode_parts(parts)
          next unless decoded

          @last_wire_size_in = parts.sum(&:bytesize)
          return decoded
        end
      rescue Protocol::ZMTP::Error
        close
        raise
      end


      # Fan-out must pass logical messages through the compression layer.
      def respond_to?(name, include_private = false)
        return false if [:write_wire, :write_wire_batch].include?(name)

        super
      end
    end
  end
end
