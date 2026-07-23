# Changelog

## [Unreleased]

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
