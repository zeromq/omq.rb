# Changelog

## Unreleased

- Initial release. Adds `ws://` and `wss://` transports to OMQ
  implementing ZeroMQ RFC 45 (ZWS 2.0). Both schemes register on
  `require "omq/transport/websocket"`.
- Subprotocols: `ZWS2.0` (no mechanism, identity-as-first-message)
  and `ZWS2.0/NULL` (NULL handshake via 0x02 command frames).
- Built on `async-websocket` for the WebSocket upgrade and
  `async-http` for the listener.
- ZWS framing: one ZeroMQ frame per WebSocket binary message,
  prefixed with a single FLAG byte (0x00 last, 0x01 more, 0x02
  command). No ZMTP/3.1 greeting.
- `wss://` is plain TLS-then-WebSocket via an `OpenSSL::SSL::SSLContext`
  passed through `tls_context:` on the socket.
- Release source is now the `zeromq/omq.rb` monorepo.
- Requires `omq ~> 0.28` and `protocol-zmtp ~> 0.10`.
