# frozen_string_literal: true

module Protocol
  module ZMTP
    # Valid socket type peer combinations per ZMTP spec.
    VALID_PEERS = {
      PAIR:    %i[PAIR].freeze,
      REQ:     %i[REP ROUTER].freeze,
      REP:     %i[REQ DEALER].freeze,
      DEALER:  %i[REP DEALER ROUTER].freeze,
      ROUTER:  %i[REQ DEALER ROUTER].freeze,
      PUB:     %i[SUB XSUB].freeze,
      SUB:     %i[PUB XPUB].freeze,
      XPUB:    %i[SUB XSUB].freeze,
      XSUB:    %i[PUB XPUB].freeze,
      PUSH:    %i[PULL].freeze,
      PULL:    %i[PUSH].freeze,
      CLIENT:  %i[SERVER].freeze,
      SERVER:  %i[CLIENT].freeze,
      RADIO:   %i[DISH].freeze,
      DISH:    %i[RADIO].freeze,
      SCATTER: %i[GATHER].freeze,
      GATHER:  %i[SCATTER].freeze,
      PEER:    %i[PEER].freeze,
      CHANNEL: %i[CHANNEL].freeze,
    }.freeze
  end
end
