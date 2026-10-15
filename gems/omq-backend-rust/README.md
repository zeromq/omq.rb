# omq-backend-rust

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-backend-rust?color=e9573f)](https://rubygems.org/gems/omq-backend-rust)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%203.3-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Rust-backed engine for [OMQ](https://github.com/zeromq/omq.rb). Same socket API,
but networking runs through the first-class
[omq-rs](https://github.com/paddor/omq.rs/tree/main/bindings/ruby) binding on
MRI and TruffleRuby, or through OMQ.java on JRuby.

## Install

The `omq-rs >= 0.2.5` dependency compiles OMQ.rs during installation on MRI
and TruffleRuby. This minimum includes the REQ/REP envelope and reply-routing
fixes in `omq-tokio` 0.28.1 and `omq-proto` 0.32.1.

JRuby uses OMQ.java on Java 25 or newer. The Java gem selects the
matching OMQ.java artifact on Linux x86_64, macOS aarch64, macOS x86_64, and
Windows x86_64.

```ruby
# Gemfile
gem "omq-backend-rust"
```

```sh
gem install omq-backend-rust
```

## Usage

```ruby
require "omq"
require "omq/backend/rust"

Async do
  push = OMQ::PUSH.new(backend: :rust)
  pull = OMQ::PULL.new(backend: :rust)

  port = pull.bind("tcp://127.0.0.1:0").port
  push.connect("tcp://127.0.0.1:#{port}")
  push.peer_connected.wait

  push << "hello from Rust"
  p pull.receive  # => ["hello from Rust"]
ensure
  push&.close
  pull&.close
end
```

Select a backend per socket to exchange ZMTP messages with Ruby and other peers.

## Supported socket types

All standard and draft ZMTP socket types: REQ/REP, PUB/SUB, PUSH/PULL,
DEALER/ROUTER, XPUB/XSUB, PAIR, CLIENT/SERVER, RADIO/DISH,
SCATTER/GATHER, CHANNEL.

## Security mechanisms

- **NULL** (default)
- **CURVE** (CurveZMQ, via [Nuckle](https://github.com/paddor/nuckle))

CURVE authenticators receive `Protocol::ZMTP::PeerInfo`, including a public key
from the configured crypto backend. As with the Ruby engine, the identity is
empty during authentication.

On JRuby, OMQ.java 0.3.0's JNI callback threads require the platform jar on the
JVM `CLASSPATH`. For example:

```sh
CLASSPATH="$HOME/.m2/repository/io/github/paddor/omq-java/0.3.0/omq-java-0.3.0-linux-x86_64.jar" jruby app.rb
```

Without it, configuring a CURVE authenticator raises `ArgumentError` before
opening a connection. CURVE without an authenticator does not need this workaround.

PLAIN and custom mechanisms raise `ArgumentError` before binding, connecting,
or sending. They are not supported by this adapter.

## Compressed transports

The Rust backend supports `lz4+tcp://` and `zstd+tcp://`. Compression
configuration uses the same bind/connect kwargs as the Ruby transports:
`level:` for zstd compression level (`-8..4`), `dict:` for a static
send-side dictionary, and `auto_dict: true` for automatic dictionary
training.

Automatic dictionary training is off by default.
OMQ.rs stores compression configuration per socket, so pass these kwargs on
the first `bind` or `connect` for a Rust-backed socket.

## Development

```sh
OMQ_RS_PATH=/path/to/omq.rs/bindings/ruby bundle install
OMQ_RS_PATH=/path/to/omq.rs/bindings/ruby bundle exec rake
```

## License

[ISC](LICENSE)
