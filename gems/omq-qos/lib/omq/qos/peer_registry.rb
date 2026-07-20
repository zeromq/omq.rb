# frozen_string_literal: true

require "async/clock"

module OMQ
  class QoS
    # Tracks sent-but-unacknowledged messages per peer for QoS >= 2.
    #
    # Keyed by {Protocol::ZMTP::PeerInfo} so entries survive reconnects:
    # when the same peer comes back, {#resume} returns the peer's pending
    # entries for retransmit on the new connection. When a peer stays away
    # longer than +dead_letter_timeout+, {#sweep_dead_letters} drains its
    # entries as {DeadLetter}s.
    #
    # Backpressure lives on the owning {OMQ::QoS} instance (an
    # {Async::Semaphore} bounded at +send_hwm+). The registry itself is a
    # pure peer-indexed store; it does not block.
    #
    class PeerRegistry
      # One pending message.
      #
      # +peer_info+ is the peer the entry is pinned to; +promise+ resolves
      # to +:delivered+ on ACK/COMP or to a {DeadLetter} on
      # dead-letter/terminal NACK/retry-exhaustion/socket-close.
      Entry = Data.define(:parts, :peer_info, :sent_at, :promise, :retry_count)


      # Per-peer state: the currently-connected {Protocol::ZMTP::Connection}
      # (or +nil+ when the peer is gone), the disconnect timestamp used by
      # the dead-letter sweep, and the digest-keyed pending map. Insertion
      # order is preserved so {#resume} can replay in original send order.
      class PeerState
        attr_accessor :connection, :disconnected_at
        attr_reader   :entries


        def initialize(connection)
          @connection      = connection
          @disconnected_at = nil
          @entries         = {}
        end
      end


      # @param capacity [Integer] send_hwm (kept for symmetry with PendingStore;
      #   the actual bound is enforced by the OMQ::QoS#send_semaphore)
      def initialize(capacity:)
        @capacity = capacity
        @peers    = {}
      end


      # Records a fresh pending entry, pinned to +peer_info+ on +conn+.
      def track(peer_info, digest, entry, conn)
        state = (@peers[peer_info] ||= PeerState.new(conn))
        state.connection = conn
        state.entries[digest] = entry
      end


      # Removes and returns the entry matching +digest+ for +peer_info+,
      # or +nil+ if unknown (e.g. late ACK after dead-letter).
      def ack(peer_info, digest)
        @peers[peer_info]&.entries&.delete(digest)
      end


      # @return [Protocol::ZMTP::Connection, nil] the live connection
      #   currently pinned to +peer_info+, if any
      def connection_for(peer_info)
        @peers[peer_info]&.connection
      end


      # Marks the peer as disconnected. Entries stay in place until either
      # the peer reconnects ({#resume}) or the dead-letter timer fires
      # ({#sweep_dead_letters}).
      def disconnect(peer_info)
        state = @peers[peer_info] or return
        state.connection      = nil
        state.disconnected_at = Async::Clock.now
      end


      # Rebinds +conn+ to +peer_info+ and returns the existing pending
      # entries in insertion order. Caller retransmits them on +conn+.
      def resume(peer_info, conn)
        state                 = (@peers[peer_info] ||= PeerState.new(conn))
        state.connection      = conn
        state.disconnected_at = nil
        state.entries.values
      end


      # Returns all entries whose peer has been gone for at least +timeout+
      # seconds, pairs them with their peer_info, and removes their
      # PeerState from the registry.
      #
      # @param now [Float]
      # @param timeout [Numeric]
      # @return [Array<Array(Entry, Protocol::ZMTP::PeerInfo)>]
      def sweep_dead_letters(now, timeout)
        expired = []
        @peers.delete_if do |peer_info, state|
          next false unless state.disconnected_at
          next false if (now - state.disconnected_at) < timeout
          state.entries.each_value { |entry| expired << [entry, peer_info] }
          true
        end
        expired
      end


      # Resolves all outstanding promises with a {DeadLetter} carrying
      # +reason+. Called from {OMQ::QoS#shutdown} so no +#wait+-ing fiber
      # hangs.
      def drain_with_dead_letter(reason)
        @peers.each do |peer_info, state|
          state.entries.each_value do |entry|
            next if entry.promise.resolved?
            dl = DeadLetter.new(
              parts:     entry.parts,
              reason:    reason,
              peer_info: peer_info,
              error:     nil,
            )
            entry.promise.resolve(dl)
          end
        end
        @peers.clear
      end


      # @return [Integer] total pending entries across all peers
      def size
        @peers.sum { |_peer, state| state.entries.size }
      end


      # @return [Boolean]
      def empty?
        @peers.all? { |_peer, state| state.entries.empty? }
      end
    end
  end
end
