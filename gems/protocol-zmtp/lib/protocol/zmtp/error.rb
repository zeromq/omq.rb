# frozen_string_literal: true

module Protocol
  module ZMTP
    # Raised on ZMTP protocol violations.
    class Error < RuntimeError
    end


    # An explicit handshake refusal. Do not retry with the same credentials.
    class FatalHandshakeError < Error
    end
  end
end
