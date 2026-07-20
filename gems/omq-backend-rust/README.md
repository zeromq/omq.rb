# omq-backend-rust

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-backend-rust?color=e9573f)](https://rubygems.org/gems/omq-backend-rust)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%203.3-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Rust-backed engine for [OMQ](https://github.com/zeromq/omq.rb). Same socket API,
but networking runs on a [Tokio](https://tokio.rs/) runtime inside a native
extension compiled via [rb_sys](https://github.com/oxidize-rb/rb-sys).

## Install

Requires a Rust toolchain (stable) at gem install time.

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
require "omq/rust"

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

The `:rust` backend is fully interoperable with the default `:ruby` backend.
Mix backends freely within the same process.

## Supported socket types

All standard and draft ZMTP socket types: REQ/REP, PUB/SUB, PUSH/PULL,
DEALER/ROUTER, XPUB/XSUB, PAIR, CLIENT/SERVER, RADIO/DISH,
SCATTER/GATHER, CHANNEL.

## Security mechanisms

- **NULL** (default)
- **CURVE** (CurveZMQ, via [Nuckle](https://github.com/paddor/nuckle))

## Development

```sh
OMQ_DEV=1 bundle install
OMQ_DEV=1 bundle exec rake
```

## License

[ISC](LICENSE)
