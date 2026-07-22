# frozen_string_literal: true

module Protocol
  module ZMTP
    module Codec

      # Frozen empty binary string for zero-length frame bodies.
      EMPTY_BINARY = "".b.freeze


      # ZMTP frame encode/decode.
      #
      # Wire format:
      #   Byte 0:   flags (bit 0=MORE, bit 1=LONG, bit 2=COMMAND)
      #   Next 1-8: size (1-byte if short, 8-byte big-endian if LONG)
      #   Next N:   body
      #
      class Frame
        FLAGS_MORE    = 0x01
        FLAGS_LONG    = 0x02
        FLAGS_COMMAND = 0x04


        # Short frame: 1-byte size, max body 255 bytes.
        SHORT_MAX = 255

        # Pre-computed single-byte flag strings (avoids Integer#chr + String#b per frame).
        FLAG_BYTES = Array.new(256) { |i| i.chr.b.freeze }.freeze


        # Encodes a multi-part message into a single wire-format string.
        # The result can be written to multiple connections without
        # re-encoding each time (useful for fan-out patterns like PUB).
        #
        # @param parts [Array<String>] message frames
        # @return [String] frozen binary wire representation
        #
        def self.encode_message(parts)
          if parts.size == 1
            s    = parts.first.bytesize
            wire = s > SHORT_MAX ? 9 + s : 2 + s
          else
            wire = 0
            j    = 0

            while j < parts.size
              s     = parts[j].bytesize
              wire += s > SHORT_MAX ? 9 + s : 2 + s
              j    += 1
            end
          end

          buf  = String.new(capacity: wire, encoding: Encoding::BINARY)
          last = parts.size - 1
          i    = 0

          while i < parts.size
            body  = parts[i]
            size  = body.bytesize
            flags = i < last ? FLAGS_MORE : 0

            if size > SHORT_MAX
              buf << FLAG_BYTES[flags | FLAGS_LONG]
              buf << [size].pack("Q>")
              buf << body
            else
              buf << FLAG_BYTES[flags]
              buf << FLAG_BYTES[size]
              buf << body
            end

            i += 1
          end

          buf.freeze
        end


        # Reads one frame from an IO-like object.
        #
        # Uses #peek to buffer just enough header bytes (2 for short frames,
        # 9 for long), then drains header + body in a single #read_exactly.
        # This is 2 calls for both short and long frames, vs the naive 3 for
        # long. A speculative read_exactly(9) would be unsafe: a <7-byte
        # short frame at idle would hang waiting for bytes that never arrive,
        # or consume bytes from the next frame on a mixed stream.
        #
        # @param io [#peek, #read_exactly]
        # @return [Frame]
        # @raise [Error] on invalid frame
        # @raise [EOFError] if the connection is closed
        def self.read_from(io, max_message_size: nil)
          buf = io.peek do |b|
            next false if b.bytesize < 2
            (b.getbyte(0) & FLAGS_LONG) == 0 || b.bytesize >= 9
          end

          raise EOFError, "Stream finished before reading frame header" if buf.bytesize < 2

          flags   = buf.getbyte(0)
          more    = (flags & FLAGS_MORE) != 0
          long    = (flags & FLAGS_LONG) != 0
          command = (flags & FLAGS_COMMAND) != 0

          if long
            raise EOFError, "Stream finished before reading long frame size" if buf.bytesize < 9

            size = (buf.getbyte(1) << 56) |
                   (buf.getbyte(2) << 48) |
                   (buf.getbyte(3) << 40) |
                   (buf.getbyte(4) << 32) |
                   (buf.getbyte(5) << 24) |
                   (buf.getbyte(6) << 16) |
                   (buf.getbyte(7) << 8)  |
                    buf.getbyte(8)
            header_size = 9
          else
            size        = buf.getbyte(1)
            header_size = 2
          end

          if max_message_size && size > max_message_size
            raise Error, "frame size #{size} exceeds max_message_size #{max_message_size}"
          end

          if size.zero?
            io.read_exactly(header_size)
            return new(EMPTY_BINARY, more: more, command: command)
          end

          wire = io.read_exactly(header_size + size)
          new(wire.byteslice(header_size, size), more: more, command: command)
        end


        # @return [String] frame body (binary)
        attr_reader :body


        # @param body [String] frame body
        # @param more [Boolean] more frames follow
        # @param command [Boolean] this is a command frame
        def initialize(body, more: false, command: false)
          @body    = body
          @more    = more
          @command = command
        end


        # @return [Boolean] true if more frames follow in this message
        def more?
          @more
        end


        # @return [Boolean] true if this is a command frame
        def command?
          @command
        end


        # Encodes to wire bytes.
        #
        # @return [String] binary wire representation (flags + size + body)
        def to_wire
          size   = @body.bytesize
          flags  = 0
          flags |= FLAGS_MORE if @more
          flags |= FLAGS_COMMAND if @command

          if size > SHORT_MAX
            buf = String.new(capacity: 9 + size, encoding: Encoding::BINARY)
            buf << FLAG_BYTES[flags | FLAGS_LONG]
            buf << [size].pack("Q>")
            buf << @body
          else
            buf = String.new(capacity: 2 + size, encoding: Encoding::BINARY)
            buf << FLAG_BYTES[flags]
            buf << FLAG_BYTES[size]
            buf << @body
          end
        end

      end
    end
  end
end
