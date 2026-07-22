# omq-zstd

[![Gem Version](https://img.shields.io/gem/v/omq-zstd?color=e9573f)](https://rubygems.org/gems/omq-zstd)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Experimental Zstandard-compressed TCP transport for
[OMQ](https://github.com/zeromq/omq.rb).

Use [`omq-lz4`](../omq-lz4) for new compressed TCP work. OMQ.rs removed
`zstd+tcp://` to reduce transport complexity after lz4rip gained
dictionary training. This gem remains for research, comparison, and
cases where zstd ratio matters more than transport simplicity.

Pick `zstd+tcp://` instead of `tcp://` and every message part on the wire is
compressed per-part with [Zstandard](https://github.com/facebook/zstd).
Compression is intrinsic to the transport: no negotiation, no socket option,
no payload changes. The ZMTP handshake runs over plain TCP. Only
post-handshake message parts are compressed.

See [RFC.md](RFC.md) for the wire-format specification and
[DESIGN.md](DESIGN.md) for the implementation rationale.

## Install

```ruby
# Gemfile
gem "omq-zstd"
```

```sh
gem install omq-zstd
```

## Usage

```ruby
require "omq"
require "omq/zstd"

pull = OMQ::PULL.new
push = OMQ::PUSH.new

uri = pull.bind("zstd+tcp://127.0.0.1:0")
push.connect(uri.to_s)

push << ["hello, compressed world"]
pull.receive  # => ["hello, compressed world"]
```

Both peers must use the `zstd+tcp://` scheme. A `tcp://` peer cannot talk to
a `zstd+tcp://` peer. They speak different transports.

### Compression level

Default is **`-3`** (negative = Zstd's fast strategy). Override at bind/connect:

```ruby
pull.bind("zstd+tcp://127.0.0.1:0", level: 3)
push.connect("zstd+tcp://127.0.0.1:5555", level: 9)
```

Per-direction, per-side: each side picks its own send level. Receiving works
at any level the peer chose.

### Dictionaries

Small messages don't compress well on their own. A shared Zstd dictionary
trained on representative payloads gives 2-10x ratios on payloads in the
dozens-to-hundreds-of-bytes range.

**User-supplied dictionary** (out-of-band agreement):

```ruby
dict = File.binread("schema.dict")  # produced by `zstd --train`
push.connect("zstd+tcp://127.0.0.1:5555", dict: dict)
```

The sender ships the dictionary to the receiver in-band as a one-shot
single-part message prefixed with the dictionary sentinel
(`37 A4 30 EC`), so the receiver does not need a copy on disk.

**Auto-trained dictionary** (zero config, the default when no `dict:` is
passed): the sender collects up to 1000 samples or 100 KiB (whichever hits
first), skipping samples larger than 2048 bytes. It trains a 2 KiB dictionary,
ships it inline, and switches to dictionary mode. Until then, payloads are
compressed without a dictionary or sent plaintext when below the threshold.

### Compression thresholds

To avoid pessimizing tiny frames, the sender skips compression below:

| Mode | Threshold |
|------|-----------|
| No dictionary | 512 B |
| With dictionary | 64 B |

Below the threshold the part is sent uncompressed (4-byte zero sentinel +
plaintext bytes).

### Security limits

The receiver bounds decompression by the socket's own `max_message_size`,
the same knob you'd use on a plain `tcp://` socket. It caps the
**total decompressed size of all parts in a single message**, not each
part individually: the budget starts at `max_message_size` and shrinks
as each part is decoded, so a message whose parts sum to more than the
cap is rejected on the offending part.

```ruby
pull.max_message_size = 1_048_576  # 1 MiB cap on the total message
```

If `max_message_size` is `nil` (OMQ's default, unlimited), there is no
ceiling on decompressed message size. Set a value that matches what
your application would tolerate over plain `tcp://`.

Independent of the message-size knob, the dictionary itself is capped at
8 KiB. A peer attempting to ship a larger dictionary, or send a message
whose decompressed parts exceed `max_message_size`, drops the connection.
`OMQ::SocketDeadError` surfaces on the next `receive`.

## Wire format

Every post-handshake ZMTP message part starts with a 4-byte sentinel:

| Sentinel (hex) | Meaning |
|---|---|
| `00 00 00 00` | Uncompressed plaintext |
| `28 B5 2F FD` | Zstandard-compressed frame |
| `37 A4 30 EC` | Dictionary shipment |

Compressed parts are standard Zstandard frames with `Frame_Content_Size`
set in the header. The receiver uses FCS for budget enforcement before
invoking the decoder. Any other leading 4 bytes close the connection.

Dictionary shipments are single-part ZMTP messages consumed by the
transport layer. They are not delivered to the application.

## Constants

| Constant | Value |
|---|---|
| Uncompressed sentinel | `00 00 00 00` |
| Zstd frame sentinel | `28 B5 2F FD` (Zstandard frame magic) |
| Dictionary sentinel | `37 A4 30 EC` |
| Default level | -3 |
| Min compress, no dict | 512 B |
| Min compress, with dict | 64 B |
| Max dictionary size | 8 KiB |
| Train max samples | 1000 |
| Train max bytes | 100 KiB |
| Train max sample length | 2048 B |
| Dictionary capacity | 2 KiB |

## When to use it

`zstd+tcp://` is worth picking when:

- You're network-bound (cross-region, IoT links, congested LAN).
- Your payloads have repetitive structure (JSON, log lines, protobuf with
  string fields, similar binary records).
- You want compression without touching the message format on either side.

It is **not** worth it for:

- `inproc://` or `ipc://`. No wire to shrink. Use `zstd+tcp://` only on
  the connections that actually need it. Other transports on the same
  socket are unaffected.
- Already-compressed payloads (gzip, video, encrypted blobs). The Zstd
  pass adds CPU for no gain.
- Latency-critical sub-microsecond paths. Compression adds single-digit
  microseconds per kilobyte at low levels, but it is not free.

## How it works (in one paragraph)

`require "omq/zstd"` registers the `zstd+tcp` scheme on
`OMQ::Engine.transports`. A `zstd+tcp` socket builds a per-engine
`Codec` (one Zstd dictionary instance shared across all the socket's
connections. Fan-out compresses each part exactly once). Each accepted
or dialed TCP connection is wrapped in `ZstdConnection`, a
`SimpleDelegator` over the underlying ZMTP connection that intercepts
`#send_message` / `#write_message` / `#receive_message`. Message parts
go out as a 4-byte sentinel + payload: `00 00 00 00` for plaintext,
`28 B5 2F FD` (Zstandard frame magic) for a compressed part, or
`37 A4 30 EC` for a one-shot single-part dictionary shipment. The
receiver dispatches on the sentinel, decompresses with bounded
buffers, and hands plaintext parts up to ZMTP unchanged.

## Development

```sh
OMQ_DEV=1 bundle install
OMQ_DEV=1 bundle exec rake test
OMQ_DEV=1 bundle exec ruby --yjit bench/level_sweep.rb
```

## License

[ISC](LICENSE)
