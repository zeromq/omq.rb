# frozen_string_literal: true

module OMQ
  class QoS
    # Prepended onto Protocol::ZMTP::Codec::Command's singleton class
    # to add ACK/NACK/CLR/COMP command builders.
    #
    module CommandClassExt
      # Builds an ACK command.
      #
      # @param hash_bytes [String] binary hash digest
      # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
      def ack(hash_bytes, algorithm: "x")
        new("ACK", "#{algorithm}#{hash_bytes}".b)
      end


      # Builds a NACK command. The on-wire +error_info+ payload is
      # +[1 byte code] [2 bytes big-endian message length] [message]+
      # per the omq-qos RFC §NACK command.
      #
      # @param hash_bytes [String] binary hash digest
      # @param code [Integer] error code byte (bit 7 = retryable)
      # @param message [String] UTF-8 error description (≤ 65535 bytes)
      # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
      def nack(hash_bytes, code: 0, message: "", algorithm: "x")
        msg_bytes  = message.b
        error_info = [code, msg_bytes.bytesize].pack("Cn") + msg_bytes
        new("NACK", "#{algorithm}#{hash_bytes}#{error_info}".b)
      end


      # Builds a CLR ("clear") command. Sent by the sender after
      # receiving an ACK (QoS 2) or COMP (QoS 3) so the receiver may
      # evict the digest from its dedup set immediately instead of
      # waiting for TTL expiry.
      #
      # @param hash_bytes [String] binary hash digest
      # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
      def clr(hash_bytes, algorithm: "x")
        new("CLR", "#{algorithm}#{hash_bytes}".b)
      end


      # Builds a COMP ("complete") command. Sent by the QoS 3 receiver
      # after its application handler returns successfully.
      #
      # @param hash_bytes [String] binary hash digest
      # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
      def comp(hash_bytes, algorithm: "x")
        new("COMP", "#{algorithm}#{hash_bytes}".b)
      end
    end


    # Prepended onto Protocol::ZMTP::Codec::Command to add
    # ACK/NACK/CLR/COMP data extraction methods.
    #
    module CommandExt
      # Extracts algorithm prefix and hash bytes from an ACK command's data.
      #
      # @return [Array(String, String)] [algorithm, hash_bytes]
      def ack_data
        algo      = @data.byteslice(0, 1)
        hash_size = Protocol::ZMTP::Codec::Command::ACK_HASH_SIZES.fetch(algo, 8)
        [algo, @data.byteslice(1, hash_size)]
      end


      # Same layout as ACK.
      #
      # @return [Array(String, String)] [algorithm, hash_bytes]
      def clr_data
        ack_data
      end


      # Same layout as ACK — COMP carries only the digest.
      #
      # @return [Array(String, String)] [algorithm, hash_bytes]
      def comp_data
        ack_data
      end


      # Extracts algorithm, hash bytes, NACK code, and error message
      # from a NACK command's data. See {CommandClassExt.nack} for the
      # wire layout.
      #
      # @return [Array(String, String, Integer, String)]
      #   [algorithm, hash_bytes, code, message]
      def nack_data
        algo      = @data.byteslice(0, 1)
        hash_size = Protocol::ZMTP::Codec::Command::ACK_HASH_SIZES.fetch(algo, 8)
        hash      = @data.byteslice(1, hash_size)
        off       = 1 + hash_size
        header    = @data.byteslice(off, 3)

        if header && header.bytesize == 3
          code, msg_len = header.unpack("Cn")
          message       = @data.byteslice(off + 3, msg_len) || "".b
        else
          code, message = 0, "".b
        end

        [algo, hash, code, message]
      end
    end
  end
end


# Known hash digest sizes by algorithm prefix.
Protocol::ZMTP::Codec::Command::ACK_HASH_SIZES = { "x" => 8, "s" => 8 }.freeze
