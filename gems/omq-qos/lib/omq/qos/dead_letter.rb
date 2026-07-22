# frozen_string_literal: true

module OMQ
  class QoS
    # Builds a {DeadLetter} for +entry+, resolves the entry's Promise
    # with it, and releases one slot on +semaphore+ so a waiting sender
    # can proceed.
    #
    # No-op if the Promise is already resolved (late ACK after
    # dead-letter, or shutdown racing a sweep).
    #
    # @param entry [PeerRegistry::Entry]
    # @param peer_info [Protocol::ZMTP::PeerInfo]
    # @param reason [Symbol] +:peer_timeout+, +:terminal_nack+,
    #   +:retry_exhausted+, or +:socket_closed+
    # @param semaphore [Async::Semaphore, nil] the send-slot semaphore
    #   to release; pass +nil+ on shutdown-drain where the whole
    #   semaphore is being torn down anyway
    # @param error [Object, nil] NackInfo at QoS 3; nil at QoS 2
    # @return [DeadLetter, nil] the built DeadLetter, or nil if the
    #   Promise was already resolved
    def self.dead_letter(entry, peer_info:, reason:, semaphore: nil, error: nil)
      return nil if entry.promise.resolved?

      dl = DeadLetter.new(
        parts:     entry.parts,
        reason:    reason,
        peer_info: peer_info,
        error:     error,
      )
      entry.promise.resolve(dl)
      semaphore&.release
      dl
    end
  end
end
