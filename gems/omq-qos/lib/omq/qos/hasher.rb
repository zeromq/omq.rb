# frozen_string_literal: true

require "xxhash"
require "digest/sha1"

module OMQ
  class QoS
    # Supported hash algorithms in default preference order.
    # x = XXH64 (8 bytes) — fast, requires xxhash library
    # s = SHA-1 truncated to 64 bits (8 bytes) — available in any standard library
    #
    # All digests are 8 bytes (64 bits). Collision probability within
    # the in-flight window (typically ≤ HWM) is negligible: ~N²/2⁶⁵
    # where N is the number of un-ACK'd messages.
    SUPPORTED_HASH_ALGOS = "xs".freeze
    DEFAULT_HASH_ALGO    = "x".freeze
    HASH_SIZE            = 8


    # Computes an 8-byte digest over raw ZMTP wire bytes.
    #
    # @param parts [Array<String>] message frames
    # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
    # @return [String] 8-byte binary digest
    #
    def self.digest(parts, algorithm: DEFAULT_HASH_ALGO)
      wire = Protocol::ZMTP::Codec::Frame.encode_message(parts)
      case algorithm
      when "x"
        [XXhash.xxh64(wire)].pack("Q<")
      when "s"
        Digest::SHA1.digest(wire).byteslice(0, 8)
      else
        raise ArgumentError, "unsupported QoS hash algorithm: #{algorithm.inspect}"
      end
    end


    # Negotiates the hash algorithm for a connection.
    # Returns the first algo in our preference list that the peer supports.
    #
    # @param peer_hash [String] peer's supported algos (e.g. "sx")
    # @return [String, nil] single-char algorithm, or nil if no overlap
    #
    def self.negotiate_hash(peer_hash)
      return DEFAULT_HASH_ALGO if peer_hash.empty?
      SUPPORTED_HASH_ALGOS.each_char do |algo|
        return algo if peer_hash.include?(algo)
      end
      nil
    end


    # Builds an ACK command for the given message.
    #
    # @param parts [Array<String>] message frames
    # @param algorithm [String] "x" (XXH64) or "s" (SHA-1 truncated)
    # @return [Protocol::ZMTP::Codec::Command]
    #
    def self.ack_command(parts, algorithm: DEFAULT_HASH_ALGO)
      Protocol::ZMTP::Codec::Command.ack(digest(parts, algorithm: algorithm), algorithm: algorithm)
    end
  end
end
