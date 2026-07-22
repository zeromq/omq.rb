# frozen_string_literal: true

require "async/semaphore"

module OMQ
  class QoS
    # Tracks sent-but-unacknowledged messages for QoS 1.
    #
    # Keyed by 8-byte XXH3 digest of the raw ZMTP wire bytes.
    #
    # Bounded by an internal {Async::Semaphore} of +capacity+ permits
    # (typically +send_hwm+). The sender acquires a permit via
    # {#wait_for_slot} before each {#track}; {#ack} and {#messages_for}
    # release permits back. This gives the QoS path the same
    # backpressure semantics the send queue has at QoS 0: once
    # +send_hwm+ messages are outstanding, the next acquire blocks until
    # an ACK (or a connection drop that drains the entries) frees a
    # slot.
    #
    class PendingStore
      Entry = Data.define(:parts, :connection, :sent_at)


      # @param capacity [Integer] max pending entries
      def initialize(capacity:)
        @entries   = {}
        @capacity  = capacity
        @semaphore = Async::Semaphore.new(capacity)
      end


      # Blocks the caller until a pending-slot permit is available and
      # then acquires it. Caller MUST follow up with a {#track}; the
      # corresponding {#ack} (or {#messages_for} drop) releases.
      def wait_for_slot
        @semaphore.acquire
      end


      def track(hash, parts, connection)
        @entries[hash] = Entry.new(
          parts:      parts,
          connection: connection,
          sent_at:    Async::Clock.now,
        )
      end


      # Acknowledges a message. Releases one semaphore permit when the
      # entry existed.
      #
      # @return [Entry, nil]
      def ack(hash)
        entry = @entries.delete(hash)
        @semaphore.release if entry
        entry
      end


      # Returns and removes all pending entries for a connection.
      # Releases one semaphore permit per removed entry.
      #
      # @return [Array<Entry>]
      def messages_for(connection)
        removed = []
        @entries.delete_if do |_hash, entry|
          if entry.connection.equal?(connection)
            removed << entry
            true
          end
        end
        removed.size.times { @semaphore.release }
        removed
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
