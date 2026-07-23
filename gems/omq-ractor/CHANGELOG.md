# Changelog

## [Unreleased]

## 0.1.7 - 2026-07-23

### Changed

- Moved release source to the `zeromq/omq.rb` monorepo.
- Require `omq ~> 0.28`.

## 0.1.6 — 2026-04-20

### Fixed

- **Rename `Transport::Inproc::DirectPipe` → `Transport::Inproc::Pipe`.**
  Upstream omq renamed the class in 0.24; the `is_a?` check in
  `install_connection_wrappers` silently fell through, leaving
  inproc without its shareable/marshal wrapper.
- **Requires omq >= 0.26.1** for the Pipe's non-String-parts
  tolerance fix.

### Changed

- Output bridge simplified: drop the `do_serialize`/`topic_type`
  branching — both paths now trust the message shape coming out of
  the Ractor port.

## 0.1.5 — 2026-04-13

### Changed

- Minor internal refactors.

## 0.1.4 — 2026-04-07

### Fixed

- **Increase Ractor handshake timeout from 100ms to 5s** — multiple Ractors
  starting simultaneously can take longer to boot and call `omq.sockets`,
  causing spurious `ArgumentError` on `-P` parallel pipe workers.

## 0.1.3 — 2026-04-07

### Fixed

- **Replace `rescue nil` with `rescue ::Ractor::ClosedError`** in
  `SocketSet#initialize` — bare `rescue nil` masked real errors. Uses
  `::Ractor::ClosedError` (root-qualified) to avoid resolving to the
  non-existent `OMQ::Ractor::ClosedError`.

### Changed

- YARD documentation on all public methods and classes.
- Code style: two blank lines between methods and constants.

## 0.1.3

### Added

- **`data:` keyword for `OMQ::Ractor.new`** — pass an arbitrary
  Ractor-shareable object into the worker block, accessible as `omq.data`.
  This is the supported way to pass configuration under Ruby 4.0's strict
  Ractor isolation, which forbids closing over outer variables.

## 0.1.2

### Added

- **`SocketSet#socket_for(port)`** — maps a `Ractor::Port` back to its
  `SocketProxy` after `Ractor.select`. Replaces manual `port == a.to_port`
  comparisons.
- **`bundler/gem_tasks`** in Rakefile — enables `rake build` and `rake release`
  for the gem release workflow.
- **README badges** — CI, gem version, license, Ruby version.

### Fixed

- **README examples handle nil on close** — all loop examples now use
  `while msg = proxy.receive` instead of bare `loop do` to avoid passing
  nil to processing functions when the socket closes.
- **`Ractor.select` example** — documents the `[port, value]` return,
  shows nil check, explains topic stripping bypass, and demonstrates
  `socket_for` for port→proxy lookup.

## 0.1.1

Initial release.
