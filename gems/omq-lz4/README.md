# omq-lz4

[![Gem Version](https://img.shields.io/gem/v/omq-lz4?color=e9573f)](https://rubygems.org/gems/omq-lz4)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

LZ4-compressed TCP transport for [OMQ](https://github.com/zeromq/omq.rb),
complementary to [`omq-zstd`](../omq-zstd).
Pick `lz4+tcp://` instead of `tcp://` or `zstd+tcp://` when you want
cheap per-message compression with a small per-connection footprint.

See [RFC.md](RFC.md) for the wire-format specification and
[CHANGELOG.md](CHANGELOG.md) for release history.

## When to pick `lz4+tcp://` over `zstd+tcp://`

LZ4 has no entropy stage (no Huffman, no FSE), ~16 KiB of encoder state
per connection, and trades a **worse compression ratio** for
**~4-8x faster encode** and **~3x less memory per connection**.

| | `zstd+tcp://` | `lz4+tcp://` |
|---|---|---|
| Encode, 1 KiB, no dict | ~3 µs | ~0.4 µs |
| Encode, 1 KiB, with dict | ~3.5 µs | ~0.5 µs |
| Memory per connection | ~256 KiB | ~16 KiB + dict |
| Ratio, 1 KiB JSON no dict | ~45% | ~65% |
| Ratio, 1 KiB JSON with dict | ~20% | ~35% |
| Auto-trained dictionaries | yes | no (user-supplied only) |

Pick `omq-lz4` for CPU- or memory-scarce deployments (edge gateways,
IoT concentrators, high-fanout scenarios where per-connection state
matters more than ratio). Pick `omq-zstd` for bandwidth-bound
deployments where CPU is cheap.

## Install

```ruby
# Gemfile
gem "omq-lz4"
```

```sh
gem install omq-lz4
```

## Usage

```ruby
require "omq"
require "omq/lz4"

pull = OMQ::PULL.new
push = OMQ::PUSH.new

uri = pull.bind("lz4+tcp://127.0.0.1:0")
push.connect(uri.to_s)

push << ["hello, compressed world"]
pull.receive  # => ["hello, compressed world"]
```

Both peers must use `lz4+tcp://`. A `tcp://` peer cannot talk to an
`lz4+tcp://` peer. They speak different transports.

### Dictionary compression

Small messages don't compress well on their own. A shared dictionary
gives 2-5x better ratios on payloads with a common prefix. Supply a
user-trained dictionary (LZ4 has no auto-training; use `omq-zstd`
for that):

```ruby
dict = File.binread("schema.dict")
push.connect("lz4+tcp://127.0.0.1:5555", dict: dict)
```

The sender ships the dictionary to the receiver in-band, prefixed
with the dictionary sentinel (`4C 5A 34 44`, "LZ4D" in ASCII), on
the first outgoing message. The receiver installs the dictionary
and decompresses subsequent messages against it. Dictionary size
is capped at **8 KiB** (same cap as `omq-zstd`).

### Compression thresholds

To avoid pessimizing tiny frames, the sender skips compression below:

| Mode            | Threshold |
|-----------------|-----------|
| No dictionary   | 512 B     |
| With dictionary | 128 B     |

Below the threshold the part is sent uncompressed (4-byte zero
sentinel + plaintext).

### Security limits

The receiver bounds decompression by the socket's `max_message_size`
(the same knob you'd use on a plain `tcp://` socket). It caps the
**total decompressed size of all parts in a single message**. A peer
attempting to send an over-budget message drops the connection.
`OMQ::SocketDeadError` surfaces on the next `receive`.

Independent of that, the dictionary itself is capped at 8 KiB; a
larger shipment drops the connection.

## Wire format

Every post-handshake ZMTP message part starts with a 4-byte sentinel:

| Sentinel (hex) | ASCII | Meaning |
|---|---|---|
| `00 00 00 00` | (none) | Uncompressed plaintext |
| `4C 5A 34 42` | `LZ4B` | LZ4-compressed single block |
| `4C 5A 34 4D` | `LZ4M` | LZ4-compressed multi-block |
| `4C 5A 34 44` | `LZ4D` | Dictionary shipment |

**Single-block** (`LZ4B`): `sentinel (4) || decompressed_size u64 LE (8) || LZ4 block bytes`.
12-byte envelope. Raw LZ4 block format (no magic, no descriptor, no
checksum). `decompressed_size` is required because LZ4 block format
carries no length prefix; the receiver pre-sizes its output buffer.

**Multi-block** (`LZ4M`): same header, followed by a sequence of
`u32 LE compressed_block_len || LZ4 block bytes` pairs. Each block
decompresses independently at up to 1 GiB. Used for parts exceeding
the single-block size cap.

**Dictionary shipment** (`LZ4D`): `sentinel (4) || dict bytes (1..8192)`.
Single-part ZMTP message consumed by the transport, not delivered
to the application. At most one per direction per connection.

Any other leading 4 bytes close the connection.

## Constants

| Constant | Value |
|---|---|
| Scheme | `lz4+tcp` |
| Uncompressed sentinel | `00 00 00 00` |
| Single-block sentinel | `4C 5A 34 42` (`LZ4B`) |
| Multi-block sentinel | `4C 5A 34 4D` (`LZ4M`) |
| Dictionary sentinel | `4C 5A 34 44` (`LZ4D`) |
| LZ4M block size | 1 GiB (`0x40000000`) |
| Max dictionary size | 8 KiB |
| Min compress, no dict | 512 B |
| Min compress, with dict | 128 B |
| LZ4B envelope | 12 B (4 sentinel + 8 size) |
| Uncompressed envelope | 4 B (sentinel only) |

## Performance

Measured on x86_64 scalar, Ruby 4.0 + YJIT, on dict-friendly (repeated
Lorem ipsum prefix) input.

**`OMQ::LZ4::Codec` (pure encode/decode, no I/O):**

| Input size | No dict encode | Dict encode | No dict decode | Dict decode |
|------------|---------------:|------------:|---------------:|------------:|
|       64 B |       ~0.9 µs  |     ~1.0 µs |       ~0.4 µs  |     ~0.6 µs |
|      256 B |       ~1.1 µs  |     ~0.8 µs |       ~0.4 µs  |     ~0.5 µs |
|    1 KiB   |       ~1.5 µs  |     ~0.9 µs |       ~0.9 µs  |     ~1.0 µs |
|   16 KiB   |       ~3.2 µs  |     ~2.4 µs |       ~3.9 µs  |     ~3.0 µs |
|    1 MiB   |      ~89 µs    |    ~87 µs   |     ~173 µs    |   ~303 µs   |

**End-to-end PUSH -> PULL over `lz4+tcp://` (loopback):**

| Message size | Throughput |
|--------------|-----------:|
|         64 B |  ~67k msg/s |
|        256 B |  ~94k msg/s |
|       1 KiB  |  ~92k msg/s |

Run the benchmarks yourself:

```sh
OMQ_DEV=1 bundle exec ruby --yjit bench/codec_micro.rb
OMQ_DEV=1 bundle exec ruby --yjit bench/transport_throughput.rb
OMQ_DEV=1 bundle exec ruby --yjit bench/head_to_head.rb   # lz4 vs zstd
```

### Head-to-head vs `omq-zstd` and plain `tcp`

End-to-end PUSH -> PULL throughput, Ruby 4.0 + YJIT. Input:
UUID-sprinkled Lorem ipsum, a fresh UUID between each Lorem
paragraph. Approximates realistic workloads where a schema
repeats but values vary (event logs, protobuf records, JSON
events), so a fraction of every message is mandatorily
incompressible.

The link between PUSH and PULL is loopback, rate-shaped with
`tc netem rate Xmbit` on `dev lo` to simulate bandwidth-limited
networks. `zstd+tcp` shown at level `-3` (default, fast) and
level `3` (tighter ratio, more CPU).

The table below: plaintext MiB/s (application-level throughput)
and wire MiB/s (bytes on the socket) at **128 KiB** payload,
across three bandwidth regimes.

| Link                | Metric   |   tcp | lz4+tcp | zstd -3 | zstd 3 |
|---------------------|----------|------:|--------:|--------:|-------:|
| **100 Mbit**        | plain    |  11.8 |     105 |     114 | **197**|
| (cap ~12 MiB/s)     | wire     |  11.8 |      12 |      12 |    12  |
|                     | speedup  | 1.00x |   8.89x |   9.70x |**16.74x**|
| **1 Gbit**          | plain    | 117   |     794 | **900** |    603 |
| (cap ~125 MiB/s)    | wire     | 117   |      93 |      94 |     36 |
|                     | speedup  | 1.00x |   6.81x |**7.73x**|  5.17x |
| **Unlimited loopback** | plain | **1 064** |  869 |    972  |    626 |
| (kernel-copy-bound) | wire     | 1 064 |      99 |     101 |     37 |
|                     | speedup  | 1.00x |   0.82x |   0.91x |  0.59x |

Three regimes visible:

- **100 Mbit**: all compressed transports saturate wire at
  ~12 MiB/s. Plaintext = wire-cap x (1 / compression-ratio). The
  tighter the ratio, the bigger the win: `zstd 3`'s 3% wire ratio
  translates to a **~17x throughput multiplier** over plain tcp.
- **1 Gbit**: compressed transports shift from wire-saturated to
  CPU-limited. `zstd -3` reaches ~75% of wire cap; `zstd 3` only
  29% (deep CPU-bound). Both beat plain tcp (which is pinned at
  the wire cap) by **6-8x**. `zstd 3`'s tighter wire no longer
  helps; there's no wire saturation to trade CPU for.
- **Unlimited loopback**: no wire cap. All three are
  CPU-limited. Plain tcp doesn't pay compression CPU, so **skip
  compression on loopback**.

Rate-shape your own link to reproduce:

```sh
sudo tc qdisc add dev lo root netem rate 100mbit  # or 1gbit, 10mbit, etc.
OMQ_DEV=1 bundle exec ruby --yjit bench/head_to_head.rb
sudo tc qdisc del dev lo root
```

Or use a `veth` pair in a network namespace so shaping doesn't
touch your host's real loopback (see `tc-netem(8)`, `ip-netns(8)`).

Full sweeps (8 sizes from 256 B to 512 KiB) for each regime live
in `bench/head_to_head.rb` output. Run it yourself; the
headline numbers above are stable across repeats but small sizes
and very large sizes vary a bit run-to-run.

**Takeaway:**

- Pick **`lz4+tcp://`** for bandwidth-limited links (any real
  network, even 1 Gbit LAN). 6-9x throughput multiplier over
  plain `tcp`, minimal memory (~16 KiB/connection), modest CPU.
  Ties or beats `zstd -3` at 1 Gbit; loses the ratio race to
  `zstd 3` at 100 Mbit and below.
- Pick **`zstd+tcp://` (level >= 3)** when the wire is the
  precious resource (100 Mbit links or slower, WAN, or you're
  paying for egress). **~17x throughput multiplier at 100 Mbit**
  for 128 KiB messages is hard to argue with.
- Pick **plain `tcp://`** when the link is *not* the bottleneck
  (localhost IPC, loopback, datacenter-fast inter-host
  connections where the bandwidth ceiling is above the CPU's
  compress/decompress speed, typically 10+ Gbit), or when the
  payload is already high-entropy (encrypted, already compressed,
  random binary) and compression only adds overhead.

## Development

```sh
OMQ_DEV=1 bundle install
OMQ_DEV=1 bundle exec rake test
```

## License

[ISC](LICENSE)
