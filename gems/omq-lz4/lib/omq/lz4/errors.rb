# frozen_string_literal: true

module OMQ
  module LZ4
    # Raised when the peer sends bytes that violate the lz4+tcp wire
    # format: unknown sentinel, a dictionary shipment that exceeds the
    # size cap, a second dictionary shipment on the same direction, a
    # per-message size-budget overrun, or a decoder failure on a
    # compressed part. The transport closes the connection on any
    # protocol error — never silently drops the offending part.
    class ProtocolError < StandardError; end
  end
end
