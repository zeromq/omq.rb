# frozen_string_literal: true

require "lz4rip"

require_relative "errors"

module OMQ
  module LZ4
    # Wire format for the lz4+tcp:// transport, encode/decode over
    # String input/output. Pure functions — no I/O, no connection state.
    # Transport (M2) owns the connection state and calls into these
    # methods per ZMTP part.
    #
    # Each wire part begins with a 4-byte sentinel:
    #
    #   00 00 00 00   uncompressed plaintext
    #   4C 5A 34 42   LZ4-compressed single block ("LZ4B" in ASCII)
    #   4C 5A 34 4D   LZ4-compressed multi-block ("LZ4M" in ASCII)
    #   4C 5A 34 44   dictionary shipment ("LZ4D" in ASCII)
    #
    # `decode_part` handles UNCOMPRESSED, LZ4B, and LZ4M. Dictionary
    # shipments are a transport-layer concern: the transport peeks the
    # first 4 bytes of each incoming wire part, routes LZ4D to
    # `decode_dict_shipment`, and never hands a shipment to `decode_part`.
    module Codec
      UNCOMPRESSED_SENTINEL = "\x00\x00\x00\x00".b.freeze
      LZ4B_SENTINEL         = "LZ4B".b.freeze
      LZ4M_SENTINEL         = "LZ4M".b.freeze
      LZ4D_SENTINEL         = "LZ4D".b.freeze

      LZ4M_BLOCK_SIZE = 1_073_741_824

      # Size thresholds below which compression isn't worth attempting.
      # Empirically tuned on Lorem-ipsum-like input via
      # bench/min_compress_size_sweep.rb: for block-format LZ4 the
      # crossover where compressed + 12-byte envelope beats
      # plaintext + 4-byte passthrough envelope sits at ~312 B without
      # a dict and ~20 B with one. We round up to 512 / 32 so the
      # machinery isn't invoked for marginal wins where real-world
      # (less repetitive) payloads would likely fall back to
      # passthrough anyway. Below the threshold, `encode_part` emits
      # UNCOMPRESSED directly without touching the compressor.
      MIN_COMPRESS_NO_DICT   = 512
      MIN_COMPRESS_WITH_DICT = 128

      # Maximum dictionary size on the wire. A policy choice, not a
      # protocol limit; tight enough that constrained peers can accept
      # dicts without allocating tens of KB of scratch.
      MAX_DICT_SIZE = 8192

      # Envelope sizes:
      #   UNCOMPRESSED = 4 (sentinel)
      #   LZ4B         = 4 (sentinel) + 8 (decompressed_size u64 LE)
      # => switching from passthrough to compressed costs 8 bytes of
      # envelope overhead. Compression must save more than that to win.
      COMPRESSED_ENVELOPE   = 12
      PASSTHROUGH_ENVELOPE  = 4

      module_function

      # Encode one plaintext part to wire bytes. Tries compression; falls
      # back to passthrough when compression wouldn't save at least the
      # envelope overhead.
      #
      # `block_codec` is an Lz4rip::BlockCodec, optionally constructed with
      # `dict: bytes`. The codec's dict presence is detected via
      # `#has_dict?` to pick the min-size threshold.
      #
      # `min_size` overrides the default threshold. Nil (the default)
      # picks `MIN_COMPRESS_NO_DICT` for a no-dict codec and
      # `MIN_COMPRESS_WITH_DICT` for a dict codec.
      def encode_part(plaintext, block_codec:, min_size: nil, block_size: LZ4M_BLOCK_SIZE)
        min_size ||= block_codec.has_dict? ? MIN_COMPRESS_WITH_DICT : MIN_COMPRESS_NO_DICT

        return encode_passthrough(plaintext) if plaintext.bytesize < min_size
        return encode_multi_block(plaintext, block_codec, block_size) if plaintext.bytesize > block_size

        compressed = block_codec.compress(plaintext)

        # Net savings = (plaintext + 4) − (compressed + 12) = plaintext − compressed − 8.
        # If ≤ 0, passthrough wins (or ties — prefer passthrough: one
        # fewer u64 for the receiver to parse).
        if compressed.bytesize + COMPRESSED_ENVELOPE >= plaintext.bytesize + PASSTHROUGH_ENVELOPE
          encode_passthrough(plaintext)
        else
          encode_compressed(plaintext.bytesize, compressed)
        end
      end

      # Decode one wire part. Returns a plaintext binary String.
      #
      # `max_size` is an optional cap on the decompressed size of this
      # single part; if the declared (LZ4B) or wire (UNCOMPRESSED)
      # plaintext size exceeds it, raises ProtocolError before any
      # decoder invocation.
      #
      # Does not handle LZ4D shipments; transport must route those to
      # `decode_dict_shipment` before calling here.
      def decode_part(wire_bytes, block_codec:, max_size: nil, block_size: LZ4M_BLOCK_SIZE)
        if wire_bytes.bytesize < 4
          raise ProtocolError, "wire part too short (< 4 bytes)"
        end

        sentinel = wire_bytes.byteslice(0, 4)
        case sentinel
        when UNCOMPRESSED_SENTINEL
          payload = wire_bytes.byteslice(4, wire_bytes.bytesize - 4)
          check_size!(payload.bytesize, max_size)
          payload
        when LZ4B_SENTINEL
          if wire_bytes.bytesize < 12
            raise ProtocolError, "LZ4B part too short (< 12 bytes, no room for size field)"
          end
          decompressed_size = wire_bytes.byteslice(4, 8).unpack1("Q<")
          if decompressed_size > block_size
            raise ProtocolError,
              "LZ4B decompressed_size #{decompressed_size} exceeds block size limit #{block_size}"
          end
          check_size!(decompressed_size, max_size)
          block = wire_bytes.byteslice(12, wire_bytes.bytesize - 12)
          begin
            block_codec.decompress(block, decompressed_size: decompressed_size)
          rescue Lz4rip::DecompressError => e
            raise ProtocolError, "LZ4B decode failed: #{e.message}"
          end
        when LZ4M_SENTINEL
          decode_multi_block(wire_bytes, block_codec, max_size, block_size)
        when LZ4D_SENTINEL
          raise ProtocolError,
            "LZ4D dictionary shipment seen at decode_part (transport should route to decode_dict_shipment)"
        else
          raise ProtocolError, "unknown sentinel #{sentinel.unpack1("H*")}"
        end
      end

      # Encode a dictionary shipment. Returns wire bytes:
      #   LZ4D sentinel (4 bytes) || dict bytes (1..8192)
      #
      # The shipment is a single-part ZMTP message (MORE flag clear)
      # from the transport's perspective, but that framing is the
      # transport's responsibility.
      def encode_dict_shipment(dict_bytes)
        validate_dict_size!(dict_bytes.bytesize)
        LZ4D_SENTINEL + dict_bytes
      end

      # Decode a dictionary shipment. Returns the dict bytes (without
      # sentinel). Raises ProtocolError if the sentinel is wrong or the
      # size is out of the [1, 8192] range.
      def decode_dict_shipment(wire_bytes)
        if wire_bytes.bytesize < 4
          raise ProtocolError, "dict shipment too short (< 4 bytes)"
        end
        sentinel = wire_bytes.byteslice(0, 4)
        unless sentinel == LZ4D_SENTINEL
          raise ProtocolError,
            "not a dict shipment (sentinel #{sentinel.unpack1("H*")}, expected 4C5A3444)"
        end
        dict = wire_bytes.byteslice(4, wire_bytes.bytesize - 4)
        validate_dict_size!(dict.bytesize)
        dict
      end

      class << self
        private

        def encode_multi_block(plaintext, block_codec, block_size)
          buf = String.new(encoding: Encoding::BINARY)
          buf << LZ4M_SENTINEL
          buf << [plaintext.bytesize].pack("Q<")

          offset = 0
          while offset < plaintext.bytesize
            chunk_size = [block_size, plaintext.bytesize - offset].min
            chunk = plaintext.byteslice(offset, chunk_size)
            compressed = block_codec.compress(chunk)
            buf << [compressed.bytesize].pack("V")
            buf << compressed
            offset += chunk_size
          end

          buf
        end


        def decode_multi_block(wire_bytes, block_codec, max_size, block_size)
          if wire_bytes.bytesize < 12
            raise ProtocolError, "LZ4M part too short (< 12 bytes, no room for size field)"
          end

          decompressed_size = wire_bytes.byteslice(4, 8).unpack1("Q<")
          check_size!(decompressed_size, max_size)

          output = String.new(capacity: decompressed_size, encoding: Encoding::BINARY)
          offset = 12
          remaining = decompressed_size

          while remaining > 0
            if offset + 4 > wire_bytes.bytesize
              raise ProtocolError, "LZ4M truncated: no room for block length at offset #{offset}"
            end

            compressed_len = wire_bytes.byteslice(offset, 4).unpack1("V")
            offset += 4

            if offset + compressed_len > wire_bytes.bytesize
              raise ProtocolError, "LZ4M truncated: block at offset #{offset} extends past wire end"
            end

            block_data = wire_bytes.byteslice(offset, compressed_len)
            offset += compressed_len

            block_decompressed_size = [block_size, remaining].min
            begin
              output << block_codec.decompress(block_data, decompressed_size: block_decompressed_size)
            rescue Lz4rip::DecompressError => e
              raise ProtocolError, "LZ4M block decode failed: #{e.message}"
            end

            remaining -= block_decompressed_size
          end

          if offset != wire_bytes.bytesize
            raise ProtocolError, "LZ4M: #{wire_bytes.bytesize - offset} leftover bytes after last block"
          end

          output
        end


        def encode_passthrough(plaintext)
          UNCOMPRESSED_SENTINEL + plaintext
        end


        def encode_compressed(decompressed_size, compressed)
          LZ4B_SENTINEL + [decompressed_size].pack("Q<") + compressed
        end


        def check_size!(declared_size, max_size)
          return unless max_size
          return if declared_size <= max_size

          raise ProtocolError,
            "part size #{declared_size} exceeds max_size #{max_size}"
        end


        def validate_dict_size!(size)
          if size < 1 || size > MAX_DICT_SIZE
            raise ProtocolError,
              "dict shipment size #{size} out of range [1, #{MAX_DICT_SIZE}]"
          end
        end
      end
    end
  end
end
