# frozen_string_literal: true

require "omq"
require "async"
require "async/http"
require "async/http/endpoint"
require "async/http/server"
require "async/websocket"
require "async/websocket/client"
require "async/websocket/adapters/http"

require_relative "websocket/version"
require_relative "websocket/options_ext"
require_relative "websocket/codec"
require_relative "websocket/connection"
require_relative "websocket/transport"

module OMQ
  module Transport
    module WebSocket

      # Raised on configuration errors specific to this transport
      # (e.g. wss:// without tls_context, malformed subprotocol).
      #
      class Error < RuntimeError
      end

    end
  end
end

OMQ::Engine.transports["ws"]  = OMQ::Transport::WebSocket
OMQ::Engine.transports["wss"] = OMQ::Transport::WebSocket

OMQ::Options.prepend(OMQ::Transport::WebSocket::OptionsExt)

OMQ::Socket.def_delegators :@options,
  :tls_context, :tls_context=,
  :ws_subprotocols, :ws_subprotocols=,
  :ws_path, :ws_path=

# OpenSSL::SSL::SSLError already covers wss:// peer aborts after
# upgrade; add it idempotently in case omq-rfc-tls hasn't been loaded.
require "openssl"
unless OMQ::CONNECTION_LOST.include?(OpenSSL::SSL::SSLError)
  OMQ::CONNECTION_LOST << OpenSSL::SSL::SSLError
end
