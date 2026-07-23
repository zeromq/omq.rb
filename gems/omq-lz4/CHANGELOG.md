# Changelog

## [Unreleased]

## 0.3.2 - 2026-07-23

### Changed

- Moved release source to the `zeromq/omq.rb` monorepo.
- Require `omq ~> 0.28` and Ruby >= 4.0.
- Require `lz4rip ~> 0.1.1`.

## 0.3.1 (2026-05-28)

### Changed

- `MIN_COMPRESS_WITH_DICT`: raised from 32 to 128. The previous value
  was too aggressive; 128 leaves a safer margin above the measured
  crossover.

## 0.3.0 (2026-05-11)

### Added

- **LZ4M multi-block encoding/decoding** (RFC §5.3a, §5.4a, §5.5 rule 4).
  Parts larger than `LZ4M_BLOCK_SIZE` (1 GiB) are split into independently
  decodable blocks, each compressed against the installed dict (if any).
  `encode_part` / `decode_part` accept a `block_size:` keyword for testing
  with smaller-than-protocol block sizes.
- `LZ4M_SENTINEL` (`"LZ4M"`) and `LZ4M_BLOCK_SIZE` (1,073,741,824) constants
  in `OMQ::LZ4::Codec`.
- LZ4B `decompressed_size` cap: the decoder now rejects single-block parts
  whose declared `decompressed_size` exceeds `LZ4M_BLOCK_SIZE` (RFC §5.5
  rule 3).
- Codec tests for LZ4M round-trips (with and without dict, partial last
  block, random bytes), malformed LZ4M inputs (truncated, leftover bytes,
  corrupt block data, budget overrun), and the LZ4B block size limit.

## 0.2.0 (2026-05-04)

### Changed

- use the Rust-backed LZ4 block codec with liblz4 bindings

## 0.1.0 (2026-04-23)

### Added

- Gem skeleton: `omq-lz4.gemspec`, `lib/omq/lz4.rb`,
  `lib/omq/lz4/version.rb`, `lib/omq/lz4/errors.rb`, `Gemfile`,
  `Rakefile`, `.gitignore`, `LICENSE` (ISC), `README.md`, CI and
  release GitHub Actions workflows, minitest harness. Depends on a
  Rust-backed LZ4 block API and `omq ~> 0.23`.
- `require "omq/lz4"` succeeds and defines `OMQ::LZ4::VERSION` and
  `OMQ::LZ4::ProtocolError`. No transport behaviour yet — the
  `lz4+tcp://` scheme registration lands in a subsequent milestone.
- **`OMQ::LZ4::Codec`** — pure wire-format encode/decode over Strings,
  no I/O, no connection state. Transport (M2) calls into these per
  ZMTP part.
  - `Codec.encode_part(plaintext, block_codec:, min_size: nil)` — tries
    compression; falls back to passthrough when compression wouldn't
    save the 8-byte envelope overhead (LZ4B envelope = 12 bytes,
    UNCOMPRESSED envelope = 4 bytes). `min_size` defaults to 64 if the
    codec has a dict installed, else 256.
  - `Codec.decode_part(wire_bytes, block_codec:, max_size: nil)` —
    handles UNCOMPRESSED (`00 00 00 00`) and LZ4B (`LZ4B` = `4C 5A 34 42`)
    sentinels. Rejects `max_size` violations before decoder
    invocation. Raises `ProtocolError` on malformed input; never
    segfaults or OOMs.
  - `Codec.encode_dict_shipment(dict_bytes)` /
    `decode_dict_shipment(wire_bytes)` — LZ4D (`LZ4D` = `4C 5A 34 44`)
    shipments. Enforces `1 ≤ dict_size ≤ 8192`.
  - Dict shipments are routed by the transport layer (not by
    `decode_part`); `decode_part` raises if it ever sees an LZ4D
    sentinel.
- **`OMQ::Transport::Lz4Tcp`** — transport plugin registering the
  `lz4+tcp://` scheme on `OMQ::Engine.transports`. Mirrors
  `omq-zstd`'s transport class hierarchy.
  - `Lz4Tcp.listener(endpoint, engine, dict: nil)`,
    `Lz4Tcp.dialer(endpoint, engine, dict: nil)`,
    `Lz4Tcp.validate_endpoint!`. Oversized dicts
    (> `OMQ::LZ4::Codec::MAX_DICT_SIZE` = 8 KiB) raise
    `OMQ::LZ4::ProtocolError` at bind/connect time.
  - `Lz4Connection` — `SimpleDelegator` over the ZMTP connection.
    Per-connection state: send `BlockCodec` (built with the
    sender-side dict if any), receive `BlockCodec` (initially no-dict,
    replaced with a dict-bound codec on receipt of a dict shipment),
    send-side "dict shipped" flag. Intercepts `#send_message`,
    `#write_message`, `#write_messages`, and `#receive_message` —
    the ZMTP handshake still runs uncompressed over raw TCP
    (`connection_class` returns the default `Protocol::ZMTP::Connection`).
  - Dict shipment is sent as a single-part ZMTP message on the first
    outgoing send when a sender-side dict is configured; the receiver
    consumes it silently (never delivered upstack) and installs a
    dict-bound `BlockCodec` for subsequent decodes. A second shipment
    on the same direction raises `ProtocolError`.
  - Per-message size budget (`engine.options.max_message_size`)
    enforced in `decode_wire_parts` — total decompressed plaintext
    across multipart parts is summed and compared to the socket's
    `max_message_size`. Dict shipments do not count against the budget.
- Integration test (`test/integration_test.rb`) covers: small (below
  threshold) and large payloads, multipart messages, dict shipment +
  subsequent compressed messages, both sides configured with a dict,
  oversized-dict rejection at bind, and a 100k-message soak (gated
  behind `OMQ_LZ4_STRESS=1`) checking for live-slot leaks after full
  GC.
- **Receiver size-budget enforcement.** `engine.options.max_message_size`
  bounds the total decompressed size of a ZMTP message, summed across
  MORE-flag-chained parts. Over-budget messages raise
  `OMQ::LZ4::ProtocolError` from the transport and close the
  connection — `receive` surfaces `OMQ::SocketDeadError` on the next
  call. The budget is enforced *before* `LZ4_decompress_safe` is
  invoked, using the `decompressed_size` field declared in the LZ4B
  envelope for compressed parts, or the wire part length for
  UNCOMPRESSED parts; lying-size inputs are caught by
  `LZ4_decompress_safe`'s own bounds check. Dictionary shipments do
  not count against the budget. Integration tests cover single-part
  and multi-part overruns, and a sub-budget multipart positive case.
- **Second-shipment rejection.** `Lz4Connection#install_recv_dict!`
  raises `OMQ::LZ4::ProtocolError` if a second LZ4D shipment arrives
  on the same direction of the same connection (dictionary is
  install-once per direction). Unit-tested by driving the private
  method directly; the transport's outgoing path never ships twice,
  so the rule primarily guards against a malicious peer.
- **Dict wrong-id detection: intentionally not implemented.** LZ4
  block format has no dictionary-id field; the transport ships dict
  bytes only (LZ4D sentinel + bytes), no id appended. A dict mismatch
  between peers produces garbage plaintext, not an error. Detect at
  the application layer if needed. See [OMQ-LZ4.plan](../OMQ-LZ4.plan)
  Open Question #1.
- **RFC.md** — wire-format specification (scheme, sentinels, part
  encoding, dict shipment, receiver budget, security considerations,
  constants). Mirrors `omq-zstd/RFC.md` structure. Status: Draft.
- **Benchmarks** in `bench/`:
  - `codec_micro.rb` — `OMQ::LZ4::Codec.encode_part` / `decode_part`
    microbench across sizes {64 B, 256 B, 1 KiB, 16 KiB, 1 MiB}, with
    and without dict. Reports wire size, ratio, compress ns,
    decompress ns.
  - `transport_throughput.rb` — end-to-end PUSH → PULL over
    `lz4+tcp://` on loopback, messages per second and µs per
    round-trip.
  - `min_compress_size_sweep.rb` — sweeps input size from 8 B to
    320 B on Lorem-ipsum-like text, reports at which size
    compressed + LZ4B envelope first beats plaintext + passthrough
    envelope, with and without a dict. Used to tune the
    `MIN_COMPRESS_*` thresholds below.
  - `head_to_head.rb` — side-by-side microbenchmark (pure codec)
    and transport throughput of `omq-lz4` vs `omq-zstd`, with and
    without a shared dictionary. Requires `omq-zstd` + `zrip` in
    the Gemfile's `:bench` group. Numbers land in README's
    "Head-to-head vs omq-zstd" section.
- **Compression thresholds tuned** from the sweep:
  - `MIN_COMPRESS_NO_DICT`: 256 → **512 B**. The measured crossover
    for Lorem-ipsum text is ~312 B; we round well above it so the
    compressor isn't invoked for marginal wins that real-world (less
    repetitive) payloads would lose anyway.
  - `MIN_COMPRESS_WITH_DICT`: 64 → **32 B**. Measured crossover with
    dict is ~20 B (the dict-reference is shorter than the literal
    payload); 32 leaves a small gap.
