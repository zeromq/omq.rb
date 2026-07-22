# Protocol::ZMTP

[![Gem Version](https://img.shields.io/gem/v/protocol-zmtp)](https://rubygems.org/gems/protocol-zmtp)
[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)

ZMTP 3.1 wire protocol: codec, connection, NULL and CURVE mechanisms.
No runtime dependencies.

## What's in the box

- **Codec::Frame**: ZMTP frame encode/decode (flags, size, body)
- **Codec::Greeting**: 64-byte greeting exchange
- **Codec::Command**: READY, PING/PONG, SUBSCRIBE, etc.
- **Connection**: per-connection frame I/O, handshake, PING/PONG
- **Mechanism::Null**: NULL security (no encryption)
- **Mechanism::Curve**: CurveZMQ (RFC 26) with pluggable crypto backend
- **Z85**: ZeroMQ RFC 32 encoding

## Usage

```ruby
require "protocol/zmtp"

# NULL mechanism, no encryption
conn = Protocol::ZMTP::Connection.new(
  io,
  socket_type: "REQ",
  mechanism: Protocol::ZMTP::Mechanism::Null.new,
)
conn.handshake!
conn.send_message(["hello"])
msg = conn.receive_message

# CURVE mechanism, pass any NaCl-compatible backend
require "protocol/zmtp/mechanism/curve"
require "nuckle"  # or: require "rbnacl"

server_mech = Protocol::ZMTP::Mechanism::Curve.server(
  public_key, secret_key, crypto: Nuckle,
)
```

## CURVE crypto backend

`Mechanism::Curve` accepts a `crypto:` parameter. Any module that
provides the NaCl API:

```ruby
# RbNaCl (libsodium, fast, constant-time)
Protocol::ZMTP::Mechanism::Curve.server(pub, sec, crypto: RbNaCl)

# Nuckle (pure Ruby, no C dependencies, don't use in production)
Protocol::ZMTP::Mechanism::Curve.server(pub, sec, crypto: Nuckle)
```

The backend must provide: `PrivateKey`, `PublicKey`, `Box`, `SecretBox`,
`Random`, `Util`, and `CryptoError`.

## License

ISC
