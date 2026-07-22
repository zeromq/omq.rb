# Changelog

## [Unreleased]

### Changed

- Moved release source to the `zeromq/omq.rb` monorepo.
- Require `omq ~> 0.28` and Ruby >= 4.0.
- Require `zrip ~> 0.1.1`.

## v0.4.2 — 2026-05-28

### Changed

- **Tuned dict training constants** to match the RFC values:
  - `MAX_DICT_SIZE` reduced from 64 KiB to 8 KiB.
  - `DICT_CAPACITY` reduced from 8 KiB to 2 KiB.
  - `TRAIN_MAX_SAMPLE_LEN` increased from 1024 to 2048 bytes.

## v0.4.1 — 2026-04-23

### Changed

- **Migrated to Zrip.** The old dict-bound codec and module-level
  compress/decompress helpers are no longer used; this release adapts
  the transport to persistent `Zrip::FrameCodec` instances.
  - `Codec#install_send_dict` now builds a fresh
    `Zrip::FrameCodec.new(dict:, level:)` per dict install (Zrip
    treats dict as a permanent codec property).
  - `ZstdConnection#install_recv_dict` does the same on the receive
    side. `@recv_codec` replaces `@recv_dict`; starts no-dict,
    rebuilt dict-bound when a shipment arrives.
  - `Codec#compress_or_plain` always dispatches to `@send_codec`
    (no more module-level fallback).
  - Auto-training now uses `Zrip::DictTrainer`, patches the generated
    dictionary id, then installs a fresh send codec.
  - Gemspec dep bumped to `zrip ~> 0.1.1`.
- Benches updated to the new API (pre-built per-level FrameCodec
  maps, trained dictionary bytes from `DictTrainer#train`).

### Notes

- No wire-format or behavioural change. The on-wire contract
  specified in `RFC.md` is unchanged; this is an internal migration
  to match the zrip API shape.

## v0.4.0 — 2026-04-18

### Changed

- **Reshaped as a full transport.** Zstd compression is no longer a
  connection wrapper negotiated via the `X-Compression` READY property —
  it is now intrinsic to a new `zstd+tcp://` transport scheme. Pick the
  scheme and every post-handshake message part is compressed; plain-TCP
  peers interoperate only if they also speak `zstd+tcp://`. The ZMTP
  handshake itself still runs in the clear (no plain/compressed
  ambiguity on the wire).

- **Namespace: `OMQ::Compression::Zstd` → `OMQ::Transport::ZstdTcp`.**
  The old `OMQ::Compression::Zstd.none/.with_dictionary/.auto` factory
  methods and the per-socket `#compression=` accessor are gone. Supply
  `level:` and `dict:` as connect/bind kwargs (or on the endpoint URI)
  instead.

- **Gem renamed `omq-rfc-zstd` → `omq-zstd`.** The `-rfc-` infix was a
  historical relic. Update your Gemfile to `gem "omq-zstd"` and your
  require to `require "omq/zstd"`. The old `require "omq/rfc/zstd"`
  entry point is removed.

- **Requires omq ~> 0.23.**

### Removed

- `X-Compression` READY property and per-peer negotiation. The transport
  scheme now carries the intent; there is nothing to advertise.
- `OMQ::Compression::Zstd::Compressor` and `Connection` wrapper classes.
- Per-socket `#compression` / `#compression=` accessors.


## v0.3.0 — 2026-04-15

### Changed

- **Requires omq ~> 0.21.**

## v0.2.0

### Changed

- Module namespace moved from `OMQ::RFC::Zstd` to `OMQ::Compression::Zstd`.
  The `require "omq/rfc/zstd"` entry point is preserved.
- Factory methods `.none`, `.with_dictionary`, `.auto` moved from the
  `Compressor` class to the `OMQ::Compression::Zstd` module itself:
  `OMQ::Compression::Zstd.none`, `.with_dictionary(dict)`, `.auto`.
- Renamed `Zstd::Compression` (class) to `Zstd::Compressor`.
- Renamed `Zstd::CompressionConnection` (wrapper) to `Zstd::Connection`.

## v0.1.2

### Changed

- `Compression#add_sample` skips the `.b` dup when the incoming
  plaintext is already a frozen binary String — OMQ's `Writable`
  mixin already hands us frozen-binary parts, so in the common
  case we stash the caller's reference instead of allocating a
  fresh copy during auto-dict training.
- `Connection#encode_parts` no longer does per-message
  `respond_to?(:auto?)` + `auto?` + `trained?` polymorphic dispatch
  on the send hot path. The "is this an auto-training compression
  that still needs samples?" check is cached at construction as
  `@auto_sampling` and flipped false the moment training completes,
  so after training the entire branch is a single instance-var read.

## v0.1.1

- New version tag

## v0.1.0

Initial release.

- RFC draft for `X-Compression` READY property and `ZDICT` command frame.
- `OMQ::Compression::Zstd::Compressor` with `.none`, `.with_dictionary`, `.auto`.
- Transparent `Connection` wrapper installed after handshake.
- Per-direction compression negotiation (RFC §7.3).
- Auto-trained dictionaries shipped over a single `ZDICT` command frame.
- Integration tests against a real OMQ socket pair.
- RFC §6.5 byte-bomb prevention: on the recv path, the decoder is handed
  the remaining `max_message_size` budget and rejects a compressed frame
  whose declared `Frame_Content_Size` exceeds the cap before any output
  allocation. Frames omitting `Frame_Content_Size` are rejected outright
  (`MissingContentSizeError`). The budget is tracked per multipart
  running total across parts. Both violations drop the connection (they
  inherit from `Protocol::ZMTP::Error`). Requires Zrip with bounded
  decompression support.
- `Compression#decompress` now accepts `max_output_size:`; the bound
  check and decode happen in a single Rust call via zrip's bounded
  decompression API.
- `:dict_auto` mode caps training samples at 1 KiB each
  (`AUTO_DICT_MAX_SAMPLE_LEN`): large frames dilute the trained
  dictionary and blow the sample budget on a handful of messages.
- **Passive sender mode (RFC Sec. 6.4).** All Compression factory
  methods (`.none`, `.with_dictionary`, `.auto`) now take a
  `passive:` keyword. A passive sender advertises its profile and
  decompresses incoming frames normally, but emits every outgoing
  frame with the uncompressed sentinel and never invokes the
  encoder. `#min_compress_bytes` returns `Float::INFINITY` in this
  mode, so `Codec.encode_part` falls through to the
  SENTINEL_UNCOMPRESSED path for every outgoing part. Passive
  senders also suppress auto-dict sample collection and ZDICT
  emission. Used by omq-cli to decompress-by-default on
  receive-capable sockets without forcing compression on peers that
  didn't opt in.
