# frozen_string_literal: true

module OMQ
  module Transport
    # Shared revision 2 envelope and inner ZMTP framing for LZ4 and Zstd.
    module CompressionArena
      HEADER_SIZE   = 16
      MAX_BYTES     = 65_536
      MAX_MESSAGES  = 256
      MAX_PARTS     = 4096
      MAX_WIRE_SIZE = HEADER_SIZE + MAX_BYTES

      # Account for the decoded String and its entry in the parts array.
      PART_CHARGE = 64

      module_function

      # Split only messages already admitted by routing. Never wait to fill
      # an arena, and leave isolated or oversized messages in per-part form.
      def each_batch(messages, target:)
        return enum_for(__method__, messages, target: target) unless block_given?

        batch = []
        bytes = 0
        parts = 0
        messages.each do |message|
          size     = message.sum { |part| part.bytesize + (part.bytesize > 255 ? 9 : 2) }
          eligible = !message.empty? && size <= target && message.size <= MAX_PARTS
          if !batch.empty? && (!eligible || bytes + size > target ||
                              parts + message.size > MAX_PARTS || batch.size == MAX_MESSAGES)
            yield batch
            batch = []
            bytes = 0
            parts = 0
          end

          if eligible
            batch << message
            bytes += size
            parts += message.size
          else
            yield [message]
          end
        end
        yield batch unless batch.empty?
      end


      def encode(messages, sentinel:, threshold:)
        data       = messages.map { |parts| Protocol::ZMTP::Codec::Frame.encode_message(parts) }.join.b
        compressed = yield(data) if data.bytesize >= threshold
        mode       = compressed && compressed.bytesize < data.bytesize ? 1 : 0
        sentinel + [mode, "\0\0\0", data.bytesize, messages.size].pack("Ca3VV") + (mode == 1 ? compressed : data)
      end


      def decode(wire, max_message_size:, error:)
        unless wire.bytesize.between?(HEADER_SIZE, MAX_WIRE_SIZE)
          raise error, "invalid arena envelope size"
        end

        mode, reserved, size, count = wire.byteslice(4, 12).unpack("Ca3VV")
        unless mode <= 1 && reserved == "\0\0\0" &&
               size.between?(1, MAX_BYTES) && count.between?(1, MAX_MESSAGES)
          raise error, "invalid arena header"
        end

        data = wire.byteslice(HEADER_SIZE..)
        data = yield(data, size) if mode == 1
        raise error, "arena decoded size mismatch" unless data.bytesize == size

        decode_messages(data, count, max_message_size, error)
      end


      def decode_messages(data, count, max_message_size, error)
        messages   = []
        parts      = []
        offset     = 0
        part_count = 0
        spent      = 0

        while offset < data.bytesize
          flags = data.getbyte(offset)
          raise error, "invalid arena frame flags" unless (flags & ~3).zero?

          header = (flags & 2).zero? ? 2 : 9
          raise error, "truncated arena frame header" if offset + header > data.bytesize

          size = header == 2 ? data.getbyte(offset + 1) : data.byteslice(offset + 1, 8).unpack1("Q>")
          offset += header
          raise error, "truncated arena frame body" if size > data.bytesize - offset

          part_count += 1
          raise error, "too many arena parts" if part_count > MAX_PARTS

          spent += size + PART_CHARGE
          if max_message_size && spent > max_message_size
            raise error, "decompressed message size exceeds maximum"
          end

          parts << data.byteslice(offset, size)
          offset += size
          next unless (flags & 1).zero?

          messages << parts
          raise error, "too many arena messages" if messages.size > count

          parts = []
          spent = 0
        end

        unless parts.empty? && messages.size == count
          raise error, "incomplete arena messages"
        end

        messages
      end
    end
  end
end
