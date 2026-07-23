# Changelog

## Unreleased

## 0.10.4 - 2026-07-23

### Changed

- Release source is now the `zeromq/omq.rb` monorepo.

## 0.10.3 — 2026-05-30

### Fixed

- **CURVE: COMMAND flag in encrypted inner byte must be 0x02, not 0x04.**
  CurveZMQ's inner plaintext flags byte uses a compact layout where
  bit 1 (0x02) means COMMAND, but encrypt/decrypt used the wire-level
  COMMAND bit (0x04). This broke interop with libzmq: a CURVE-encrypted
  SUBSCRIBE from a libzmq SUB was treated as data and silently dropped,
  so the PUB never registered the subscription.

## 0.10.2 — 2026-05-28

- Bump version (was missed in 0.10.1).

## 0.10.1 — 2026-05-28

### Added

- **`Connection#write_wire_batch`** writes multiple pre-encoded wire
  byte strings under a single mutex acquisition, amortizing lock
  overhead for fan-out send pumps.

### Changed

- **Single-write short frames.** Frames with bodies <= 255 bytes now
  combine the 2-byte header and body into one `@io.write` call,
  halving per-frame io-stream mutex overhead (+5-7% small-message
  TCP throughput).

### Fixed

- **NULL mechanism greeting: `as-server` field is now always 0.**
  RFC 23 requires `as-server = 0` for the NULL mechanism, but the
  server side was forwarding the caller's flag (`0x01`). This broke
  interoperability with strict ZMTP 3.x peers that validate the field.

## 0.10.0 — 2026-04-20

### Added

- **`PeerInfo` extended with `identity`.** `PeerInfo = Data.define(:public_key, :identity)`
  now bundles both post-handshake peer anchors. CURVE authenticators
  still receive a `PeerInfo` during auth (with `identity: ""`, since
  identity arrives post-auth).
- **`Connection#peer_info`.** After a successful handshake, returns a
  frozen `PeerInfo` combining the peer's CURVE public key (if any) and
  `ZMQ_IDENTITY`. Usable directly as a Hash key. Nil before handshake.
  Upper layers (e.g. omq-qos levels 2/3) use the whole value as a
  stable per-peer identifier across reconnects.
- **Mechanism return hashes carry `peer_public_key`.** Both
  `Mechanism::Null#handshake!` and `Mechanism::Curve` handshake paths now
  return `peer_public_key:` alongside `peer_identity:` etc. (nil for
  NULL; the peer's long-term `crypto::PublicKey` for CURVE).

### Changed

- **Drop hot-path binary-encoding coercion and frame-body freezes.**
  `Frame#initialize` and `Frame.encode_message` no longer copy non-binary
  bodies via `String#b` — callers (OMQ's `Writable#send`, ZMTP command
  builders) already hand in binary bytes, and for an outgoing message the
  receiver (`String.new(capacity:) << body`) treats any encoding as raw
  bytes once the buffer itself is binary. `Connection#receive_message`
  also stops freezing each frame body and the returned parts array; the
  caller decides what to freeze. Net effect: fewer allocations per
  message and no per-part `.freeze` on the receive path.

## 0.9.0 — 2026-04-18

### Changed

- **`Frame.read_from` uses `io.peek` to collapse the long-frame read
  path.** The previous implementation issued 2 `read_exactly` calls for
  short frames (header + body) and 3 for long frames (2-byte header,
  remaining 7 size bytes, body). It now peeks just enough header bytes
  (2 for short, 9 for long), decodes the size from the peek buffer, and
  drains header + body in a single `read_exactly(header_size + size)`.
  Long frames drop to the same 2-call read path as short frames; the
  `read_long_size` helper is gone. A speculative `read_exactly(9)` would
  not be safe here — a <7-byte short frame at idle would hang waiting
  for bytes that never arrive, or steal bytes from the next frame on a
  mixed stream.

- **Transport IO must now also respond to `#peek`** in addition to
  `#read_exactly`, `#write`, `#flush`, and `#close`. `io-stream`'s
  `Async::IO::Stream` already provides this; custom transport wrappers
  need a `#peek(&block)` that yields the current read buffer and fills
  it until the block returns truthy.

## 0.8.1 — 2026-04-16

### Changed

- **`Frame#initialize` skips redundant `.b` copy.** Body is now kept
  as-is when it is already `Encoding::BINARY`, avoiding a per-frame
  String allocation on the read path (`read_exactly` and `byteslice`
  already return binary strings).

- **`Frame#to_wire` uses `String.new(capacity:)` with `<<` appends**
  instead of `+` concatenation, reducing from 3 intermediate String
  allocations per frame to 1 pre-sized buffer.

- **`Frame.encode_message` pre-computes wire size** for the output
  buffer `capacity:` hint, avoiding re-allocations during fan-out.
  Single-part messages (the common case) skip the iteration and
  inline the size calculation.

- **`Command.ping` / `.pong` default to `EMPTY_BINARY`** instead of
  `"".b`, which allocated a mutable copy on every call despite
  `frozen_string_literal: true`. `#ping_ttl_and_context` uses
  `EMPTY_BINARY` for the empty-context fallback.

- **`Subscription.body` drops redundant outer `.b`** — both operands
  are already binary, so the concatenation result is binary without
  a second encoding conversion.

## 0.8.0 — 2026-04-15

### Added

- **`Codec::Subscription`** — helper module that unifies ZMTP 3.0
  message-form (`\x01`/`\x00` + prefix data frame) and ZMTP 3.1
  command-form (`SUBSCRIBE`/`CANCEL` command frame) subscription
  encodings. `.body(prefix, cancel:)` builds the message-form body;
  `.parse(frame)` returns `[:subscribe|:cancel, prefix]` for either
  form, letting upper layers accept both without branching.
- **`Connection#peer_major` / `#peer_minor`** — ZMTP wire revision of
  the peer, captured from the greeting. Lets upper layers pick the
  subscription wire form (and other version-gated features) per peer.
  Populated by `Mechanism::Null`, `Mechanism::Plain`, and
  `Mechanism::Curve` via their handshake result hash.
- **`Codec::Greeting.read_from(io)`** — reads the 11-byte signature
  phase first, validates the revision byte, then reads the rest of
  the 64-byte greeting. A ZMTP/2.0 peer (revision `0x01`) is now
  rejected loudly with `unsupported ZMTP revision 0x01 (ZMTP/2.x);
  need revision >= 3`, instead of hanging forever in `read_exactly`
  waiting for bytes that never arrive.

### Changed

- **Greeting error messages** refer to the ZMTP *revision byte*
  (`0x03`) rather than a "version #{major}.#{minor}" — the byte at
  offset 10 is the wire revision, which only accidentally matches the
  spec major in ZMTP/3.x.

## 0.7.1 — 2026-04-14

### Changed

- **Short-frame read fast path.** `Frame.read_from` now fetches the
  2-byte header (flags + first size byte) in a single `read_exactly`
  call instead of two separate 1-byte reads. Short frames (≤255 bytes,
  the vast majority of ZMTP traffic) now hit a 2-call read path
  (header + body) instead of 3. Long frames read the remaining 7 size
  bytes via the extracted `read_long_size` helper.

## 0.7.0 — 2026-04-13

### Added

- **Extension metadata hook on the handshake mechanisms.** `Mechanism::Null`,
  `Mechanism::Plain`, and `Mechanism::Curve` expose a `metadata` accessor
  (`Hash{String => String}`) that upper layers can populate before
  `#handshake!`. Any entries are merged into the outgoing READY properties
  (and INITIATE, for CURVE/PLAIN client side). `Codec::Command.ready` gained
  a matching `metadata:` kwarg. Used by `omq-rfc-zstd` to advertise the
  `X-Compression` property without forking the handshake code path.

- **`Connection#peer_properties`.** The full peer READY property hash is
  now retained after a successful handshake (previously only Socket-Type,
  Identity, and the X-QoS pair were extracted). Extensions can inspect
  the peer's advertised properties to negotiate optional features.
  Returned by all three mechanisms as `peer_properties:` in the
  handshake result hash.

## 0.6.0 — 2026-04-12

### Changed

- **Consolidated empty-binary constant.** `Codec::Frame::EMPTY_BODY`
  and `Codec::Command::EMPTY_DATA` are gone, replaced by a single
  `Codec::EMPTY_BINARY` shared across the codec module. Anything
  referencing the old constants needs to switch to
  `Protocol::ZMTP::Codec::EMPTY_BINARY`.

## 0.5.1 — 2026-04-10

### Changed

- **Reduced allocations on hot paths.** Pre-computed `FLAG_BYTES` lookup
  table eliminates `Integer#chr` + `String#b` per frame. `encode_message`
  inlines wire encoding instead of creating throwaway Frame objects per
  part. `write_frames` and `Command#to_body` skip redundant `.b` when
  strings are already binary. `EMPTY_BODY` and `EMPTY_DATA` constants
  replace per-call `"".b` allocations.

## 0.5.0 — 2026-04-10

### Fixed

- **Wire writes are now cancellation-safe.** `Connection#send_message`,
  `#write_message`, `#write_messages`, `#write_wire`, and `#send_command`
  each wrap their `@mutex.synchronize` block in
  `Async::Task#defer_cancel`. A ZMTP frame is two `@io.write` calls
  (header then body); under a socket-level barrier cascade (`barrier.stop`)
  an `Async::Cancel` could previously land between those two writes,
  leaving the peer with a header pointing at a body that never arrives
  and an unrecoverable framer desync. `defer_cancel` holds the
  cancellation until the write finishes, so cascading teardown only
  unwinds at frame boundaries. Mutex protection is unchanged — it
  guards against thread races; `defer_cancel` guards against fiber
  cancellation. Non-Async callers (e.g. tests) fall through to the
  unwrapped path.

## 0.4.0 — 2026-04-09

### Added

- **`Connection#write_messages`** — batched multipart send for work-stealing
  send pumps. Dequeue a batch at once and write it under a single mutex
  acquisition instead of one lock per message. Amortizes lock overhead on
  high-throughput paths.

### Changed

- **Zero-alloc frame headers on the unencrypted hot send path.**
  `#write_frames` used to allocate, per part, a `Codec::Frame` object, a
  `.b` body copy, a 1-or-9-byte header String, and a concatenated wire
  String (a copy of the entire body just to glue the header on). The body
  copy was the dominant allocation — every 64 KB send produced a 64 KB
  throwaway String, feeding the GC at ~1.2 GB/s under sustained load.
  Headers are now packed into a reusable `@header_buf` via
  `Array#pack(buffer:)`, header and body are written separately
  (io-stream buffers them into one syscall per batch), the `Frame`
  object is gone, and the body is no longer copied. `@mechanism.encrypted?`
  is hoisted out of the per-frame loop. The encrypted path is unchanged —
  CURVE/BLAKE3ZMQ still authenticate over the full encoded frame and need
  a single wire String per part.

  Bench impact downstream in omq (push_pull + router_dealer, 1 and 3 peers,
  all transports): 48 improvements, 0 regressions across 72 measurements.
  Peaks: `router_dealer tcp 3p 64B +40.7%`, `push_pull ipc 1p 64 KB +28.8%`
  (crosses 1.3 GB/s).

## 0.3.0 — 2026-04-07

- Replace `[Async {}].each(&:wait)` with `Barrier` in tests.
- YARD documentation on all public methods and classes.
- Code style: two blank lines between methods and constants.
- Fix `#read_frame` decryption for non-CURVE encrypted mechanisms
  (e.g. BLAKE3ZMQ). Previously only CURVE's `\x07MESSAGE`-wrapped command
  frames were decrypted; inline-encrypted command frames (SUBSCRIBE, PING,
  etc.) were silently dropped, breaking PUB/SUB over BLAKE3ZMQ.

- **Breaking:** `Mechanism::Curve` API is now kwargs-only:
  `Curve.server(public_key:, secret_key:, crypto:)` and
  `Curve.client(server_key:, crypto:)`. Client keys are optional — when
  omitted, an ephemeral permanent keypair is auto-generated. INITIATE
  always contains `C + vouch + metadata` per RFC 26.
- **Breaking:** Authenticator now receives a `Protocol::ZMTP::PeerInfo`
  (with a `crypto::PublicKey`) via `#call`. The `#include?` duck-typing
  is removed. Sends an ERROR command to the client on rejection.
- Add `Protocol::ZMTP::PeerInfo` shared across mechanisms.
- Add `#maintenance` to `Mechanism::Curve` for automatic cookie key rotation.
  Returns `{ interval: 60, task: <Proc> }` on server-side mechanisms so the
  host application can rotate the cookie key every 60 seconds, limiting the
  forward secrecy exposure window.

## 0.2.0

- Add `Mechanism::Plain` — PLAIN authentication (RFC 24). Carries username and
  password in a `HELLO` command during the handshake; no frame encryption.
  Accepts an optional `authenticator:` callable on the server side for
  credential validation.

## 0.1.2

- Check frame size against `max_message_size` before reading the body from the
  wire. Previously, the entire frame was allocated into memory before the size
  check, allowing a malicious peer to cause arbitrary memory allocation with a
  single oversized frame header.
- Size limit applies to all frames including commands — an attacker cannot bypass
  the check by setting the command flag.

## 0.1.1

- Initial public release.
