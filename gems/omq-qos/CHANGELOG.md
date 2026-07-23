# Changelog

## [Unreleased]

## 0.3.2 - 2026-07-23

### Changed

- Moved release source to the `zeromq/omq.rb` monorepo.
- Require `omq ~> 0.28`.

## 0.3.1 — 2026-04-20

### Changed

- Test suite speedup (4.8s → 1.4s): tightened `read_timeout` and
  `dead_letter_timeout` values in tests that exercise asynchrony,
  and fixed two replay tests whose break conditions always fell
  through to the timeout.

## 0.3.0 — 2026-04-20

### Added

- **QoS 2 — exactly-once with peer pinning and dedup.** Builds on
  QoS 1's ACK/retry machinery with two additions:
  - *Peer pinning.* Each message is pinned to the peer it was first
    sent to (keyed by post-handshake `PeerInfo` — CURVE public key
    or `ZMQ_IDENTITY`). On mid-flight disconnect the message waits
    `dead_letter_timeout` seconds for the same peer to return
    before being dead-lettered. This prevents the at-least-once
    case of "peer A got it, peer B also got it after reconnect
    retargeted".
  - *Receiver dedup set.* Per-connection, TTL- and HWM-bounded set
    of XXH64 content hashes. Duplicate deliveries (replay after a
    mid-flight disconnect) are ACK'd again but suppressed from the
    application — using the new recv-pump nil-drop contract from
    omq 0.25.

- **QoS 3 — exactly-once + application-level completion.** QoS 2
  plus receiver-side COMP/NACK: `#receive` takes a block whose
  return value becomes a `COMP` command and whose raised
  `StandardError` becomes a `NACK` carrying an error code. Senders
  retry retryable NACKs with exponential backoff (up to
  `max_retries`), and dead-letter on terminal NACKs, retry
  exhaustion, or `processing_timeout` expiry.

- **Promise-based `#send` return at QoS >= 1.** `socket.send(parts)`
  returns an `Async::Promise` that resolves to `:delivered` on
  successful ACK/COMP, or to an `OMQ::QoS::DeadLetter` Data
  (`parts`, `reason`, `peer_info`, `error`) on terminal failure.
  Pattern-match on the outcome:
  ```ruby
  case push.send(parts).wait
  in :delivered                 then :ok
  in OMQ::QoS::DeadLetter => dl then retry_queue << dl.parts
  end
  ```

- **`OMQ::QoS` builder API.**
  - `OMQ::QoS.at_least_once(hash_algos: …)`
  - `OMQ::QoS.exactly_once(dead_letter_timeout:, dedup_ttl:, …)`
  - `OMQ::QoS.exactly_once_and_processed(max_retries:,
    processing_timeout:, retry_backoff:, …)`

- **New components:** `DedupSet` (TTL + HWM eviction), `PeerRegistry`
  (peer pinning via `PeerInfo`), `DeadLetter` data class,
  `ErrorCodes` (NACK reason taxonomy), `RetryScheduler`
  (exponential backoff with a `retry_backoff` Range bound).

- **RFC: "Comparison to MQTT QoS 2" section.** Explains why this
  spec uses a 3-way `MESSAGE` → `ACK` → `CLR` exchange with
  hash-based dedup rather than MQTT's 4-way
  `PUBLISH`/`PUBREC`/`PUBREL`/`PUBCOMP` with scarce 16-bit packet
  IDs. Covers the narrow collision tradeoff and the application-
  level nonce recipe for byte-identical-but-distinct messages.

### Changed

- **Breaking: `OMQ::QoS` is now a class, not a module.** It holds
  per-socket configuration *and* runtime state (peer registry,
  dedup sets, semaphore, dead-letter sweep task).

- **Breaking: `socket.qos=` takes `nil` or an `OMQ::QoS`
  instance.** Integer assignment (`qos = 1`) is rejected with
  `ArgumentError` — use `OMQ::QoS.at_least_once` instead. Each
  `OMQ::QoS` is socket-scoped; a second `attach!` raises.

- **Bounded backpressure via `Async::Semaphore`.** The
  bounded-slot wait replaces the hand-rolled Notification + while
  loop from 0.2.

- **Requires omq ~> 0.25.** QoS 2's dedup-hit suppression relies
  on the new recv-pump nil-drop contract added in omq 0.25.

### Removed

- **Fan-out QoS dropped.** PUB/SUB, XPUB/XSUB, and RADIO/DISH are
  no longer QoS targets. Assigning any non-nil `qos` to a fan-out
  socket raises `ArgumentError` at the setter. ACK-per-subscriber
  on fan-out was always a poor fit (no meaningful retry target,
  per-message hash state explodes with N subscribers) — the RFC
  and README now state this explicitly. QoS 1–3 remain supported
  on PUSH/PULL, SCATTER/GATHER, and REQ/REP.

- **Gem renamed to `omq-qos`** (from `omq-rfc-qos`). Require path
  moves from `omq/rfc/qos` to `omq/qos`; library code relocated
  from `lib/omq/rfc/qos/` to `lib/omq/qos/`. The `rfc/` namespace
  was an unnecessary layer — this is a plugin gem, not a spec
  repository.

### Fixed

- **DirectPipe short-circuit is now QoS-aware.** Inproc peers
  still skip QoS hooks (delivery is synchronous, ACKs are not
  meaningful), but the decision now recognises an `OMQ::QoS`
  instance in `options.qos` as well as an Integer level.


## 0.2.0 — 2026-04-15

### Changed

- **Requires omq ~> 0.21.** Routing extensions track the shared-queue
  recv path introduced in omq 0.21 (no more `FairQueue` / `SignalingQueue`).

### Fixed

- **Rebuilt against current omq routing API.** Routing extensions now
  match the post-0.20 omq contracts (`RoundRobinExt`, `PushExt`,
  `ScatterExt`, `PullExt`, `GatherExt`, `FanOutExt`, `SubExt`,
  `XSubExt`, `DishExt`).
- **Inproc DirectPipe short-circuit.** QoS hooks now skip DirectPipe
  peers (inproc delivery is synchronous; ACKs are not meaningful there).
- **ACK command dispatch** now goes through a new `ConnectionExt`
  prepend on `Protocol::ZMTP::Connection` (`qos_on_command` hook),
  avoiding any change to core omq's recv pump.
- `EngineExt` removed; handshake QoS negotiation lives in a new
  `LifecycleExt` prepended onto `Engine::ConnectionLifecycle`.
- **REQ no longer double-enqueues on mid-flight disconnect.** The old
  `ReqExt` override re-enqueued the pending request on top of
  `RoundRobinExt`'s pending-store replay, and incorrectly flipped
  `@state` back to `:ready` while a request was still outstanding.
  `RoundRobinExt` already handles the replay; `ReqExt` is removed.

### Added

- **Bounded pending store with backpressure.** `PendingStore` now takes
  a `capacity:` (sized to `send_hwm`) and exposes `#wait_for_slot`.
  `RoundRobinExt#write_batch` waits for a free slot before sending, so a
  peer that stops ACKing stalls the sender instead of growing the store
  unboundedly. `#ack` and `#messages_for` signal an
  `Async::Notification` to wake blocked senders.
- Multi-message in-flight replay test for QoS 1 PUSH/PULL.
- SIGKILL peer-process replay test for QoS 1 PUSH/PULL (forks a child
  PULL, kills it hard, asserts pending messages land on a backup).
- README sections on backpressure and `linger: 0` semantics.

### Changed

- Test suite rewritten for the omq 0.20 socket API (setter-based
  `linger`/`qos`/`reconnect_interval`, top-level `SocketError`, explicit
  `SUB#subscribe`).

## 0.1.1 — 2026-04-07

- YARD documentation on all public methods and classes.
- Code style: expand `else X` one-liners, two blank lines between methods
  and constants.

## 0.1.0

Initial release.

### Added

- **QoS 1 (at-least-once)** delivery via ACK command frames and xxHash/SHA-1
  message identification.
- **Hash algorithm negotiation** — `X-QoS-Hash` READY property. Peers
  advertise supported algorithms in preference order; first common match
  is used per connection. No overlap → connection dropped.
- **Strict QoS matching** — peers MUST advertise the same QoS level.
  Mismatch drops the connection immediately (no silent fallback to QoS 0).
- **Supported algorithms** — `x` (XXH64, 8 bytes) and `s` (SHA-1 truncated
  to 64 bits). Future algorithms MAY use different digest sizes.
- **Socket types** — PUSH/PULL, SCATTER/GATHER, PUB/SUB, XPUB/XSUB,
  RADIO/DISH (ACK command frames), REQ/REP (reply = ACK, retry on
  disconnect).
- **PendingStore** — tracks sent-but-unACK'd messages per routing strategy.
  On connection loss, unacked messages are re-enqueued for the next peer.
- **Zero overhead at QoS 0** — all prepends check `engine.options.qos`
  and fall through to original behavior.
- **RFC** — `rfc/zmtp-qos.md` specifying wire format, handshake
  properties, per-socket-type behavior, and security considerations.
