# frozen_string_literal: true

module Protocol
  module ZMTP
    # Z85 encoding/decoding (ZeroMQ RFC 32).
    #
    # Encodes binary data in printable ASCII using an 85-character alphabet.
    # Input length must be a multiple of 4 bytes; output is 5/4 the size.
    #
    module Z85
      CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#".freeze
      DECODE = Array.new(128, -1)
      CHARS.each_byte.with_index { |b, i| DECODE[b] = i }
      DECODE.freeze

      BASE = 85


      # Encodes binary data to a Z85-encoded ASCII string.
      #
      # @param data [String] binary data (length must be a multiple of 4)
      # @return [String] Z85-encoded string (5/4 the size of the input)
      # @raise [ArgumentError] if data length is not a multiple of 4
      def self.encode(data)
        data = data.b
        raise ArgumentError, "data length must be a multiple of 4 (got #{data.bytesize})" unless (data.bytesize % 4).zero?

        out = String.new(capacity: data.bytesize * 5 / 4)
        i = 0
        while i < data.bytesize
          value = data.getbyte(i) << 24 | data.getbyte(i + 1) << 16 |
                  data.getbyte(i + 2) << 8 | data.getbyte(i + 3)
          4.downto(0) do |j|
            out << CHARS[(value / (BASE**j)) % BASE]
          end
          i += 4
        end
        out
      end


      # Decodes a Z85-encoded string back to binary data.
      #
      # @param string [String] Z85-encoded string (length must be a multiple of 5)
      # @return [String] decoded binary data (4/5 the size of the input)
      # @raise [ArgumentError] if string length is not a multiple of 5 or contains invalid characters
      def self.decode(string)
        raise ArgumentError, "string length must be a multiple of 5 (got #{string.bytesize})" unless (string.bytesize % 5).zero?

        out = String.new(capacity: string.bytesize * 4 / 5, encoding: Encoding::BINARY)
        i = 0
        while i < string.bytesize
          value = 0
          5.times do |j|
            byte = string.getbyte(i + j)
            d = byte < 128 ? DECODE[byte] : -1
            raise ArgumentError, "invalid Z85 character: #{string[i + j].inspect}" if d == -1
            value = value * BASE + d
          end
          out << ((value >> 24) & 0xFF).chr
          out << ((value >> 16) & 0xFF).chr
          out << ((value >> 8) & 0xFF).chr
          out << (value & 0xFF).chr
          i += 5
        end
        out
      end
    end
  end
end
