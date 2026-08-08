# Changelog

## [Unreleased]

## [0.2.0] - 2026-08-08

### Added

- Added TruffleRuby support for the Rust backend.
- Added a shared fallback file descriptor watcher for Rubies without a
  native `Fiber.scheduler`.
- Added TruffleRuby CI coverage for the Rust backend.

### Changed

- Updated to `omq-tokio` 0.21.1 and `omq-proto` 0.25.1.
- Replaced Magnus with a small `rb-sys` helper layer that uses public
  Ruby C API calls.

### Fixed

- Raised Ruby exceptions through public C API entry points for
  TruffleRuby compatibility.
- Rejected invalid negative numeric options before they can overflow Rust
  sizes or panic on invalid durations.

## [0.1.7] - 2026-08-02

### Added

- Restored Rust/Ruby backend interop coverage.

### Changed

- Updated to `omq-tokio` 0.21.0.

### Fixed

- Drained the Ruby-to-Tokio send pump before native socket close.
- Released Rust send ring slots before awaiting native sends, avoiding
  high-throughput stalls under `omq-tokio` 0.21.0 backpressure.
- Waited for Rust peer registration before resolving `peer_connected`.

## [0.1.6] - 2026-08-01

### Added

- Updated to `omq-tokio` 0.20.3 and `omq-proto` 0.25.0.
- Enabled OMQ.rs `zstd+tcp://` by default.
- Forwarded compression bind/connect kwargs to current OMQ.rs options:
  `dict:`, `auto_dict:`, `compression_threshold:`, `max_recv_dict_size:`,
  `compression_offload_threshold:`, and zstd `level:`.

## [0.1.5] - 2026-07-31

### Fixed

- Used `IO#wait_readable` for Rust backend receive waits and routed
  `close_read` wakeups through the native receive notifier.
- Updated to `omq-tokio` 0.20.2, fixing large byte-stream `PUB` fan-out
  delivery over IPC and TCP.

## [0.1.4] - 2026-07-30

### Fixed

- Accepted Ruby string identities when materializing Rust sockets.

## [0.1.3] - 2026-07-30

### Added

- Added `require "omq/backend/rust"` as the canonical backend load path.

### Removed

- Removed the old `require "omq/rust"` load path.

## 0.1.2 - 2026-07-28

### Changed

- Updated to `omq-tokio` 0.20.1 and `omq-proto` 0.24.1.
- Marked the native extension Ractor-safe for Ruby 4 Ractors.
- Adapted CURVE server setup to `CurveServerOptions`.

## 0.1.1 - 2026-07-23

### Changed

- Updated to `omq-tokio` 0.19.3 and `omq-proto` 0.23.2.
- Removed obsolete Blake3ZMQ support.
- Moved release source to the `zeromq/omq.rb` monorepo.

## v0.1.0 — 2026-06-24

Initial release.

### Added

- **`OMQ::Rust::Engine`** — drop-in OMQ engine backed by omq-tokio. Pass
  `backend: :rust` to any OMQ socket constructor.
- All standard socket types: REQ/REP, PUB/SUB, PUSH/PULL, DEALER/ROUTER,
  XPUB/XSUB, PAIR.
- All draft socket types: CLIENT/SERVER, RADIO/DISH, SCATTER/GATHER, CHANNEL.
- TCP and IPC transports.
- CURVE (CurveZMQ) and BLAKE3ZMQ security mechanisms.
- Full cross-backend interop with the default Ruby engine.
- Lifecycle promises: `peer_connected`, `all_peers_gone`, `subscriber_joined`.
- Monitor event forwarding from the Tokio runtime.
- Configurable IO thread count via `OMQ::Rust.io_threads`.
