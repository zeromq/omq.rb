# frozen_string_literal: true

require "delegate"

require_relative "../../lz4/codec"

module OMQ
  module Transport
    module Lz4Tcp
      # Per-connection state + encode/decode hooks. A SimpleDelegator over
      # the ZMTP connection so send_message / write_message /
      # receive_message route through compression, but everything else
      # (write_command, close, etc.) passes through untouched.
      class Lz4Connection < SimpleDelegator
        DEFAULT_DICT_CAPACITY = 2048
        DEFAULT_TRAIN_TRIGGER = 100

        # @return [Integer, nil] wire bytesize of the last received
        #   message (sum across parts, compressed sentinel included).
        attr_reader :last_wire_size_in

        def initialize(conn, send_dict_bytes:, max_message_size:, auto_dict: nil)
          super(conn)
          @max_message_size = max_message_size

          @recv_codec       = build_block_codec(nil)
          @recv_dict_bytes  = nil
          @last_wire_size_in = nil

          if send_dict_bytes
            @send_dict_bytes   = send_dict_bytes.b
            @send_codec        = build_block_codec(@send_dict_bytes)
            @send_dict_shipped = false
            @trainer           = nil
          elsif auto_dict
            @send_dict_bytes   = nil
            @send_codec        = build_block_codec(nil)
            @send_dict_shipped = true
            @trainer           = Lz4rip::DictTrainer.new(auto_dict[:capacity])
            @train_trigger     = auto_dict[:trigger]
            @train_msg_count   = 0
          else
            @send_dict_bytes   = nil
            @send_codec        = build_block_codec(nil)
            @send_dict_shipped = true
            @trainer           = nil
          end
        end


        def send_message(parts)
          maybe_train!(parts)
          wire = encode_parts(parts)
          ship_send_dict!
          __getobj__.send_message(wire)
        end


        def write_message(parts)
          maybe_train!(parts)
          wire = encode_parts(parts)
          ship_send_dict!
          __getobj__.write_message(wire)
        end


        def write_messages(messages)
          messages.each { |parts| maybe_train!(parts) }
          wires = messages.map { |parts| encode_parts(parts) }
          ship_send_dict!
          __getobj__.write_messages(wires)
        end


        def receive_message
          # Loop: a dict shipment is consumed silently and we read the
          # next ZMTP message. Only data messages are returned to the
          # caller. Budget tracking happens inside decode_wire_parts.
          loop do
            parts   = __getobj__.receive_message
            decoded = decode_wire_parts(parts)
            if decoded
              @last_wire_size_in = parts.sum(&:bytesize)
              return decoded
            end
          end
        end


        private


        def build_block_codec(dict_bytes)
          if dict_bytes
            Lz4rip::BlockCodec.new(dict: dict_bytes)
          else
            Lz4rip::BlockCodec.new
          end
        end


        def maybe_train!(parts)
          return unless @trainer

          parts.each do |pt|
            bytes = pt.is_a?(String) && pt.encoding == Encoding::BINARY ? pt : pt.to_s.b
            @trainer.add_sample(bytes)
          end
          @train_msg_count += 1
          return if @train_msg_count < @train_trigger

          finish_training!
        end


        def finish_training!
          dict_bytes = @trainer.train
          @trainer = nil

          return if dict_bytes.empty?

          @send_dict_bytes   = dict_bytes.b.freeze
          @send_codec        = build_block_codec(@send_dict_bytes)
          @send_dict_shipped = false
        end


        def encode_parts(parts)
          parts.map do |pt|
            bytes = pt.is_a?(String) && pt.encoding == Encoding::BINARY ? pt : pt.to_s.b
            LZ4::Codec.encode_part(bytes, block_codec: @send_codec)
          end
        end


        def ship_send_dict!
          return if @send_dict_shipped

          shipment = LZ4::Codec.encode_dict_shipment(@send_dict_bytes)
          __getobj__.write_message([shipment])
          @send_dict_shipped = true
        end


        # Returns an array of plaintext parts, or nil if the whole
        # ZMTP message was a dict shipment (consumed silently).
        #
        # Budget tracking is per-message (sum of decompressed sizes
        # across parts). Dict shipment parts do not count against the
        # budget — they aren't messages.
        def decode_wire_parts(parts)
          decoded   = []
          all_dicts = true
          budget    = @max_message_size

          parts.each do |wire|
            raise LZ4::ProtocolError, "wire part too short (< 4 bytes)" if wire.bytesize < 4

            sentinel = wire.byteslice(0, 4)
            if sentinel == LZ4::Codec::LZ4D_SENTINEL
              install_recv_dict!(wire)
              next
            end

            all_dicts = false
            plaintext = LZ4::Codec.decode_part(wire, block_codec: @recv_codec, max_size: budget)
            budget -= plaintext.bytesize if budget
            decoded << plaintext
          end

          all_dicts ? nil : decoded
        end


        def install_recv_dict!(wire)
          if @recv_dict_bytes
            raise LZ4::ProtocolError, "second dictionary shipment on the same direction"
          end

          dict_bytes = LZ4::Codec.decode_dict_shipment(wire)
          # Replace the no-dict recv codec with a dict-bound one. The
          # old codec is GC'd. Lz4rip treats dict as a permanent codec
          # property, so install-time is always a fresh build.
          @recv_codec      = Lz4rip::BlockCodec.new(dict: dict_bytes)
          @recv_dict_bytes = dict_bytes
        end

      end
    end
  end
end
