# frozen_string_literal: true

require "omq/transport/compressed_connection"

module OMQ
  module Transport
    module ZstdTcp
      class ZstdConnection < CompressedConnection
        # @return [Integer, nil] wire bytesize of the last received message
        #
        attr_reader :last_wire_size_in


        def initialize(conn, codec)
          super(conn, max_message_size: codec.max_message_size,
                arena_sentinel: Codec::ARENA_MAGIC, protocol_error: ProtocolError)
          @codec              = codec
          @dict_shipped       = false
          # FrameCodec is the decoder. Starts no-dict; when a dict
          # shipment arrives on this direction, we build a fresh
          # dict-bound FrameCodec and replace this one.
          @recv_codec         = Zrip::FrameCodec.new
          @recv_no_dict_codec = @recv_codec
          @recv_dict_bytes    = nil
          @last_wire_size_in  = nil
        end


        def send_message(parts)
          compressed = @codec.compress_parts(parts)
          ship_dict!
          __getobj__.send_message(compressed)
        end


        def write_message(parts)
          compressed = @codec.compress_parts(parts)
          ship_dict!
          __getobj__.write_message(compressed)
        end


        def write_messages(messages)
          compressed = messages.map { |parts| @codec.compress_parts(parts) }
          ship_dict!
          __getobj__.write_messages(compressed)
        end


        private


        def decompress_arena(data, size)
          unless data.start_with?(Codec::ZSTD_MAGIC) && @codec.parse_frame_content_size(data) == size
            raise ProtocolError, "arena Frame_Content_Size mismatch"
          end

          decode_zstd_frame(data, size)
        end


        def ship_dict!
          return if @dict_shipped

          dict_bytes = @codec.send_dict_bytes
          return unless dict_bytes

          __getobj__.write_message([dict_bytes])
          @dict_shipped = true
        end


        def decode_parts(parts)
          budget    = @codec.max_message_size
          decoded   = []
          all_dicts = true

          parts.each do |wire|
            if wire.start_with?(Codec::ZDICT_MAGIC) && parts.size != 1
              raise ProtocolError, "dictionary must be a single-part message"
            end

            plaintext = decode_part(wire, budget)
            if plaintext
              all_dicts = false
              budget -= plaintext.bytesize if budget
              decoded << plaintext
            end
          end

          all_dicts ? nil : decoded
        end


        def decode_part(wire, budget)
          raise ProtocolError, "short frame" if wire.bytesize < 4

          head = wire.byteslice(0, 4)

          case head
          when Codec::NUL_PREAMBLE
            plaintext = wire.byteslice(4, wire.bytesize - 4) || "".b
            enforce_budget!(plaintext.bytesize, budget)
            plaintext
          when Codec::ZSTD_MAGIC
            decode_zstd_frame(wire, budget)
          when Codec::ZDICT_MAGIC
            install_recv_dict(wire)
            nil
          else
            raise ProtocolError, "unrecognized preamble: #{head.unpack1('H*')}"
          end
        end


        def decode_zstd_frame(wire, budget)
          fcs = @codec.parse_frame_content_size(wire)
          raise ProtocolError, "Zstd frame missing Frame_Content_Size" if fcs.nil?

          if budget && fcs > budget
            raise ProtocolError, "declared FCS #{fcs} exceeds limit #{budget}"
          end

          @codec.validate_frame!(wire)
          codec = frame_has_dict_id?(wire) ? @recv_codec : @recv_no_dict_codec
          plaintext = codec.decompress(wire, max_output_size: fcs)
          raise ProtocolError, "Zstd decoded size mismatch" unless plaintext.bytesize == fcs

          plaintext
        rescue Zrip::DecompressError => e
          raise ProtocolError, "decompression failed: #{e.message}"
        rescue Zrip::MissingContentSizeError => e
          raise ProtocolError, "Zstd frame missing Frame_Content_Size (#{e.message})"
        rescue Zrip::OutputSizeLimitError => e
          raise ProtocolError, "declared FCS exceeds limit (#{e.message})"
        end


        def frame_has_dict_id?(wire)
          return false if wire.bytesize < 5

          (wire.getbyte(4) & 0x03) != 0
        end


        def install_recv_dict(wire)
          raise ProtocolError, "second dictionary shipment on the same direction" if @recv_dict_bytes

          if wire.bytesize < 8
            raise ProtocolError, "dict frame too short"
          end

          if wire.bytesize > Codec::MAX_DICT_SIZE
            raise ProtocolError, "dict exceeds #{Codec::MAX_DICT_SIZE} bytes"
          end

          @recv_codec      = Zrip::FrameCodec.new(dict: wire.b)
          @recv_dict_bytes = wire.b
        end


        def enforce_budget!(size, budget)
          return if budget.nil?
          return if size <= budget

          raise ProtocolError, "decompressed message size exceeds maximum"
        end

      end
    end
  end
end
