# frozen_string_literal: true

require "delegate"

module OMQ
  module Transport
    module ZstdTcp
      class ZstdConnection < SimpleDelegator
        # @return [Integer, nil] wire bytesize of the last received message
        #
        attr_reader :last_wire_size_in


        def initialize(conn, codec)
          super(conn)
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


        def receive_message
          loop do
            parts   = __getobj__.receive_message
            decoded = decode_parts(parts)
            if decoded
              @last_wire_size_in = parts.sum { |p| p.bytesize }
              return decoded
            end
          end
        end


        def respond_to?(name, include_private = false)
          return false if name == :write_wire
          super
        end


        private


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

          decompress_opts = budget ? { max_output_size: budget } : {}

          codec = frame_has_dict_id?(wire) ? @recv_codec : @recv_no_dict_codec
          codec.decompress(wire, **decompress_opts)
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
