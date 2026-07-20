# Plan: lz4rip frame-dict support + new zrip gem

Two parallel backend tracks that unblock `omq-rfc-lz4` v1 and re-open the
Zstd-vs-LZ4 question on fair terms. Both result in Ruby gems with the
**same** public interface, so swapping algorithms in benchmarks and in the
RFC sentinel table is mechanical.

The user will execute these in a separate session. This file is the
self-contained brief.

## Shared interface (target for both gems)

```ruby
# One-shot frame format (no dictionary)
Backend.compress(bytes)            # => String (binary)
Backend.decompress(bytes)          # => String (binary)

# Persistent dictionary handle, Ractor-shareable
dict = Backend::Dictionary.new(bytes)
dict.id                            # => Integer (u32, derived from sha256(bytes)[0..4], LE)
dict.size                          # => Integer (bytes)
dict.compress(plaintext)           # => String — frame format with Dict_ID set
dict.decompress(frame_bytes)       # => String — verifies Dict_ID matches dict.id

Backend::DecompressError           # raised on bad input / dict mismatch
```

Critical properties:

- `#compress` / `#decompress` go through a **persistent** native context
  (no per-call allocation of encoder/decoder state). This is the whole
  point of zrip existing instead of just using `zstd-ruby`.
- Output of `dict.compress` is a real, standards-compliant frame (LZ4
  frame format / Zstd frame format) that the reference C tools can
  decode given the same dictionary.
- Ractor-shareable: `Dictionary` instances are frozen and may be passed
  across Ractors, mirroring `lz4rip` 0.1.x today.

## Track A — fork lz4_flex, fix dict-bound frame support, ship lz4rip 0.2.0

### A1. Fork upstream

- Fork `PSeitz/lz4_flex` → `paddor/lz4_flex`.
- Branch: `frame-dict-support`.
- Baseline: latest 0.13.x release.

### A2. Add real dictionary-bound frame encode/decode

The block-level machinery already exists in `lz4_flex`:
`decompress_internal::<true, _>` in `src/block/decompress.rs` accepts an
`ext_dict` parameter, and the block encoder has equivalent
`compress_with_dict` paths. The gap is purely at the **frame** layer.

Two changes:

1. **Decoder** — `src/frame/decompress.rs:139-142` currently hardcodes
   `Error::DictionaryNotSupported` whenever the FLG.DictID flag is set.
   Replace this with: read the `Dict_ID` from the FrameDescriptor,
   look up the user-supplied dictionary on the `FrameDecoder`, verify
   the id matches, then thread the dictionary bytes into each block's
   `decompress_internal` call as `ext_dict`. New API:
   ```rust
   FrameDecoder::with_dictionary<R: Read>(reader: R, dict: &[u8], dict_id: u32) -> Self
   ```
   Decoder errors out if a frame's `Dict_ID` does not match `dict_id`.

2. **Encoder** — `FrameEncoder` currently treats `FrameInfo::dict_id` as
   metadata only (it writes the field but compresses without the dict).
   Add:
   ```rust
   FrameEncoder::with_dictionary<W: Write>(writer: W, dict: &[u8], dict_id: u32) -> Self
   ```
   Encoder sets FLG.DictID, writes `dict_id` into the FrameDescriptor,
   and calls the existing `compress_with_dict` path on every block.

Tests in the fork:

- Round-trip: encode with dict, decode with same dict → original.
- Negative: encode with dict A, decode with dict B → `DictIdMismatch`.
- Negative: decode with no dict → `DictionaryRequired`.
- **Interop**: encode in Rust, decode with reference `lz4` CLI given
  the same dictionary file (`lz4 -d -D dict.bin`). And vice versa.
  This is the proof that we are emitting standards-compliant frames.

### A3. Cut lz4rip 0.2.0 against the fork

In `/home/roadster/dev/oss/omq/lz4rip`:

- `Cargo.toml`: depend on `lz4_flex` via git fork
  (`{ git = "https://github.com/paddor/lz4_flex", branch = "frame-dict-support" }`)
  pending upstream PR merge.
- Rewrite `Lz4rip::Dictionary` to back onto `FrameEncoder::with_dictionary` /
  `FrameDecoder::with_dictionary`. Output is now a real LZ4 frame, not
  the proprietary `size_le_u32 || lz4_block` blob.
- Add `Lz4rip::Dictionary#id` → `u32` derived from `sha256(bytes)[0..4]`
  interpreted little-endian. This is the `Dict_ID` written into every
  emitted frame.
- Add `Lz4rip::Dictionary#size`.
- Bump to **0.2.0** (breaking: wire output format changed).
- Update README and CHANGELOG: explicitly call out that 0.1.x output is
  not interoperable with 0.2.x output, and that 0.2.x is interoperable
  with the reference LZ4 CLI when both sides use the same dict file.
- Tests: round-trip, dict id derivation, interop with `lz4` CLI in CI.

### A4. Upstream PR (non-blocking)

Open `paddor/lz4_flex#frame-dict-support` → `PSeitz/lz4_flex` PR. Until
merged, `lz4rip` 0.2.x stays pinned to the fork branch. Once merged and a
new `lz4_flex` release is cut, flip lz4rip's `Cargo.toml` back to crates.io.

## Track B — new zrip gem

### B1. Scaffold

New repo `/home/roadster/dev/oss/omq/zrip`, mirroring `/home/roadster/dev/oss/omq/lz4rip` exactly:

```
zrip/
├── Cargo.toml
├── Gemfile
├── README.md
├── CHANGELOG.md
├── LICENSE                 # MIT (match lz4rip)
├── Rakefile
├── zrip.gemspec
├── ext/zrip/
│   ├── Cargo.toml
│   ├── extconf.rb
│   └── src/lib.rs
├── lib/zrip.rb
├── lib/zrip/version.rb
└── test/test_zrip.rb
```

Same toolchain as lz4rip: `magnus` + `rb-sys` + `rake-compiler`. Ractor-safe.
Ruby >= 3.3.

### B2. Backend: killingspark/zstd-rs

Pure-Rust Zstandard implementation, no C libzstd dependency, no bindgen.
User confirmed it supports dictionary-based compression.

`Cargo.toml`:
```toml
[dependencies]
ruzstd = { version = "0.x", features = ["std"] }   # decoder
zstd_rs = { version = "0.x" }                      # encoder, if separate crate
magnus = "0.7"
```
(Crate names TBC at scaffold time — `killingspark/zstd-rs` may be a single
crate covering both directions.)

Open question to resolve at B2 start: confirm `killingspark/zstd-rs`
exposes a **persistent encoder context** (i.e. an encoder you can
construct once and reuse across many `compress` calls without
reallocating internal buffers). If not, file the issue upstream and
either contribute the API or fall back to the C `libzstd` via FFI as
plan B (gives up the pure-Rust property but unblocks the benchmark).

### B3. API

Mirror Track A's shared interface 1:1, just substitute `Zrip` for `Lz4rip`:

```ruby
Zrip.compress(bytes, level: 1)
Zrip.decompress(bytes)

dict = Zrip::Dictionary.new(bytes)
dict.id        # u32 from sha256(bytes)[0..4] LE  — same scheme as lz4rip
dict.size
dict.compress(plaintext, level: 1)
dict.decompress(frame_bytes)

Zrip::DecompressError
```

Inside the Rust extension: build the encoder/decoder context **once** per
`Dictionary` (or once per Ractor for the no-dict path) and reuse it across
calls. The whole reason this gem exists is that `zstd-ruby` allocates
~256 KB of `ZSTD_CCtx` / `ZSTD_DCtx` state per call, which dominates
small-message benchmarks and biases the LZ4-vs-Zstd comparison.

Non-goals (match `lz4rip`):
- Streaming
- Multi-threaded compression
- High-compression-level presets beyond what `level:` already covers
- Encoding preservation (always returns binary strings)

### B4. Tests

- Round-trip with and without dict.
- Dict id derivation.
- Persistent-context check: pathological loop (100k × 64 B compress)
  must not allocate per-iteration native state. Measure with
  `GC.stat(:total_allocated_objects)` delta and a Rust-side counter if
  needed.
- Interop with reference `zstd` CLI: encode in Ruby with a dict, decode
  with `zstd -d -D dict.bin`. And vice versa.

## Track C — wire it back into omq-rfc-lz4

Sequencing: needs A3 done. B3 is independent and only feeds the
benchmark / RFC §3.3 update.

### C1. Re-run benchmarks fairly

- Update `bench/compression_shootout.rb` to use `zrip` instead of
  `zstd-ruby`. The persistent-context property of zrip makes this a
  fair fight.
- Re-run on the same lorem-ipsum sizes (64 B / 256 B / 1 KB / 4 KB /
  16 KB), 20k iterations.
- Keep the existing `lz4rip` rows but switch them to the 0.2.x dict-bound
  frame API (so the benchmark numbers reflect the actual wire format
  the RFC will use).
- Capture dict-on and dict-off rows for both algorithms.

### C2. Update RFC §3.3

Replace the current "biased benchmark" caveat with the fair numbers.
Possible outcomes:

- LZ4 still dominates → keep RFC §3.3 as "LZ4 only in v1", note that
  Zstd was retested fairly and lost.
- Zstd-1 + dict beats LZ4 + dict on the target sizes → write a follow-up
  RFC reserving sentinel `28 B5 2F FD` (Zstd frame magic, LE) and a
  `zstd:dict:sha256:<hex>` profile, with the same negotiation rules.
  `omq-rfc-lz4` v1 still ships LZ4-only; the Zstd RFC is layered on top.

Either way: drop the "biased" language, link the new bench output, keep
the `dict:sha256:<hex>` profile as the v1 baseline.

### C3. Finalize omq-rfc-lz4 v1 implementation

With `lz4rip` 0.2.x available:

- `lib/omq/rfc/lz4/compression.rb`: change `#compress` to call
  `@dictionary.compress(plaintext)` (now emits a real LZ4 frame with
  `Dict_ID`) and `#decompress` to call `@dictionary.decompress(body)`
  (verifies `Dict_ID` against `@dictionary.id` internally).
- Drop the legacy `size_le_u32` byte-peek in `#decompress`'s
  `max_message_size` check; replace it with reading the frame's
  `Content_Size` field via a new `Lz4rip::Frame.peek_content_size(bytes)`
  helper added in lz4rip 0.2.x.
- `lib/omq/rfc/lz4/codec.rb`: dispatch on `SENTINEL_LZ4_FRAME`
  (`04 22 4D 18`) only.
- Tests: round-trip, `Dict_ID` mismatch raises, `max_message_size`
  enforced before decode, interop against an `lz4` CLI peer that
  encodes a frame with the same dict.

## Verification across all tracks

- `cd lz4rip && bundle exec rake` passes; CHANGELOG mentions 0.2.0 break.
- `cd zrip && bundle exec rake` passes.
- Both gems' interop tests pass against their respective reference CLIs.
- `cd omq-rfc-lz4 && OMQ_DEV=1 bundle exec ruby bench/compression_shootout.rb`
  produces fair numbers.
- `omq-rfc-lz4` RFC.md §3.3 cites the new numbers, no "biased" caveat.
- `omq-rfc-lz4` test suite passes against lz4rip 0.2.x.

## Out of scope for this plan

- Upstreaming `frame-dict-support` to `PSeitz/lz4_flex` (file the PR but
  do not block on review).
- The Zstd follow-up RFC, if benchmarks justify one — separate plan.
- Migrating `omq-cli` off its in-formatter LZ4 — separate follow-up,
  already noted in `omq-rfc-lz4/RFC.md` §9.3.
