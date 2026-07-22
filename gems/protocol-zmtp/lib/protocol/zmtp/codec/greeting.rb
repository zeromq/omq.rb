# frozen_string_literal: true

module Protocol
  module ZMTP
    module Codec
      # ZMTP 3.1 greeting encode/decode.
      #
      # The greeting is always exactly 64 bytes:
      #   Offset  Bytes  Field
      #   0       1      0xFF (signature start)
      #   1-8     8      0x00 padding
      #   9       1      0x7F (signature end)
      #   10      1      major version
      #   11      1      minor version
      #   12-31   20     mechanism (null-padded ASCII)
      #   32      1      as-server flag (0x00 or 0x01)
      #   33-63   31     filler (0x00)
      #
      module Greeting
        SIZE             = 64
        # Bytes 0..10 cover the 10-byte signature plus the major-version
        # byte at offset 10. ZMTP 2.0 peers only send an 11-byte signature
        # phase (their full greeting is shorter than 64), so we must
        # sniff the major version before committing to reading 64 bytes.
        SIGNATURE_SIZE   = 11
        SIGNATURE_START  = 0xFF
        SIGNATURE_END    = 0x7F
        VERSION_MAJOR    = 3
        VERSION_MINOR    = 1
        MECHANISM_OFFSET = 12
        MECHANISM_LENGTH = 20
        AS_SERVER_OFFSET = 32


        # Encodes a ZMTP 3.1 greeting.
        #
        # @param mechanism [String] security mechanism name (e.g. "NULL")
        # @param as_server [Boolean] whether this peer is the server
        # @return [String] 64-byte binary greeting
        def self.encode(mechanism: "NULL", as_server: false)
          buf = "\xFF".b + ("\x00" * 8) + "\x7F".b
          buf << [VERSION_MAJOR, VERSION_MINOR].pack("CC")
          buf << mechanism.b.ljust(MECHANISM_LENGTH, "\x00")
          buf << (as_server ? "\x01" : "\x00")
          buf << ("\x00" * 31)
        end


        # Reads and decodes a ZMTP 3.x greeting from +io+, sniffing the
        # major version after the first 11 bytes so a ZMTP 2.0 peer
        # (which would never send the full 64 bytes) is detected and
        # rejected without blocking forever on +read_exactly+.
        #
        # @param io [#read_exactly]
        # @return [Hash] { major:, minor:, mechanism:, as_server: }
        # @raise [Error] on invalid signature or unsupported version
        #
        def self.read_from(io)
          sig   = io.read_exactly(SIGNATURE_SIZE).b
          major = sig.getbyte(10)

          unless sig.getbyte(0) == SIGNATURE_START && sig.getbyte(9) == SIGNATURE_END
            raise Error, "invalid greeting signature"
          end

          unless major >= 3
            raise Error, "unsupported ZMTP revision 0x%02x (ZMTP/%d.x); need revision >= 3" %
                         [major, major == 1 ? 2 : major]
          end

          decode(sig + io.read_exactly(SIZE - SIGNATURE_SIZE))
        end


        # Decodes a ZMTP greeting.
        #
        # @param data [String] 64-byte binary greeting
        # @return [Hash] { major:, minor:, mechanism:, as_server: }
        # @raise [Error] on invalid greeting
        def self.decode(data)
          raise Error, "greeting too short (#{data.bytesize} bytes)" if data.bytesize < SIZE

          data = data.b

          unless data.getbyte(0) == SIGNATURE_START && data.getbyte(9) == SIGNATURE_END
            raise Error, "invalid greeting signature"
          end

          major = data.getbyte(10)
          minor = data.getbyte(11)

          unless major >= 3
            raise Error, "unsupported ZMTP revision 0x%02x (need revision >= 3)" % major
          end

          mechanism = data.byteslice(MECHANISM_OFFSET, MECHANISM_LENGTH).delete("\x00")
          as_server = data.getbyte(AS_SERVER_OFFSET) == 1

          {
            major:     major,
            minor:     minor,
            mechanism: mechanism,
            as_server: as_server,
          }
        end
      end
    end
  end
end
