# frozen_string_literal: true

module Protocol
  module ZMTP
    # Post-handshake identity of the peer on a {Connection}.
    #
    # - +public_key+ is the peer's CURVE long-term +crypto::PublicKey+,
    #   or +nil+ when the mechanism does not authenticate a key (e.g. NULL).
    # - +identity+ is the peer's +ZMQ_IDENTITY+ string, or +""+ when no
    #   identity was advertised in the READY metadata.
    #
    # Used as a peer key by upper layers (e.g. omq-qos levels 2/3): the
    # whole value object is frozen and equality-comparable, so it can be
    # stored directly in a Hash without picking one anchor over the other.
    #
    # Also passed to CURVE authenticators during the handshake (at that
    # point +identity+ is +""+, since it arrives post-auth).
    PeerInfo = Data.define(:public_key, :identity)
  end
end
