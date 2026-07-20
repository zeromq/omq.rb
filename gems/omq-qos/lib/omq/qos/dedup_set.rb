# frozen_string_literal: true

require "async/clock"

module OMQ
  class QoS
    # Per-receiving-connection dedup set for QoS >= 2.
    #
    # An ordered map of +{digest => added_at}+ keyed by the 8-byte XXH64 /
    # SHA-1 digest. Hash insertion order gives us oldest-first eviction
    # for free.
    #
    # Entries are added on message delivery, removed eagerly on CLR from
    # the sender, and swept lazily on TTL expiry via {#sweep}. When a new
    # entry would exceed +capacity+ the oldest entry is evicted — the
    # sender will not retransmit past +dead_letter_timeout+ anyway, so an
    # eviction older than that is safe.
    #
    class DedupSet
      # @param capacity [Integer] typically recv_hwm; maximum entries
      #   before oldest-first eviction kicks in
      def initialize(capacity:)
        @capacity = capacity
        @entries  = {}
      end


      # @param digest [String] 8-byte binary
      def seen?(digest)
        @entries.key?(digest)
      end


      # Adds +digest+ and stamps it with the current monotonic clock.
      # Evicts the oldest entry if capacity would be exceeded.
      def add(digest)
        if @entries.size >= @capacity && !@entries.key?(digest)
          @entries.shift
        end
        @entries[digest] = Async::Clock.now
      end


      # Removes an entry (called on CLR from the sender).
      def remove(digest)
        @entries.delete(digest)
      end


      # Drops entries older than +ttl+ seconds at +now+.
      def sweep(now, ttl)
        cutoff = now - ttl
        @entries.delete_if { |_digest, added_at| added_at < cutoff }
      end


      # @return [Integer]
      def size
        @entries.size
      end


      # @return [Boolean]
      def empty?
        @entries.empty?
      end
    end
  end
end
