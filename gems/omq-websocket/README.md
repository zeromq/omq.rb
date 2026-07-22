# OMQ::Transport::WebSocket

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-websocket?color=e9573f)](https://rubygems.org/gems/omq-websocket)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%203.3-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

ZeroMQ-over-WebSocket transport for [OMQ](https://github.com/zeromq/omq.rb).
Implements [ZeroMQ RFC 45](https://rfc.zeromq.org/spec/45/) (ZWS 2.0).

Adds `ws://` and `wss://` schemes to OMQ. Both register at
`require` time. No other OMQ code change required.

## Install

```ruby
gem "omq",           "~> 0.28"
gem "omq-websocket", "~> 0.1"
```

## Use

```ruby
require "omq"
require "omq/websocket"

# Server
pull = OMQ::PULL.new
pull.bind("ws://127.0.0.1:5555")

# Client
push = OMQ::PUSH.connect("ws://127.0.0.1:5555")
push.send("hello")
```

### TLS (`wss://`)

```ruby
ctx = OpenSSL::SSL::SSLContext.new
ctx.cert = OpenSSL::X509::Certificate.new(File.read("cert.pem"))
ctx.key  = OpenSSL::PKey::RSA.new(File.read("key.pem"))

pull = OMQ::PULL.new
pull.tls_context = ctx
pull.bind("wss://127.0.0.1:5556")
```

### Mechanism

By default the client offers `ZWS2.0/NULL` then `ZWS2.0`. The server
picks the first one it accepts. Override via:

```ruby
sock.ws_subprotocols = %w[ZWS2.0]   # force no-mechanism
```

### Path

Defaults to `/`. Override via `sock.ws_path = "/zeromq"`. Listener
returns 404 for any other path.

## Wire protocol

Per RFC 45, each ZeroMQ frame maps to one WebSocket binary message:
a single FLAG byte (`0x00` final, `0x01` more, `0x02` command)
followed by the frame body. No 64-byte ZMTP greeting. Mechanism
negotiation happens via `Sec-WebSocket-Protocol` during the HTTP
upgrade. See `lib/omq/transport/websocket/codec.rb`.

## License

ISC.
