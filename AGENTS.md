# AGENTS.md

OMQ is ZeroMQ (ZMTP 3.1) in pure Ruby on Async fibers. No C extensions, no FFI
in the core. It must interoperate with libzmq, pyzmq, CZMQ and omq.rs.

## Layout

- `lib/omq/`: socket types, `Engine`, `Routing::*` strategies, transports,
  `Reactor` (IO thread for non-Async callers).
- `gems/protocol-zmtp`: ZMTP codec, handshake, NULL/PLAIN/CURVE. No runtime
  dependencies.
- `gems/omq-backend-libzmq`, `gems/omq-backend-rust`: alternative engines,
  selected per socket with `backend: :libzmq` / `:rust`.
- `gems/omq-lz4`, `gems/omq-zstd`, `gems/omq-websocket`: transport plugins
  (`lz4+tcp://`, `zstd+tcp://`, `ws://`).
- `gems/omq-cli`, `gems/omq-ractor`: CLI and Ractor bridge.
- `test/omq/`: core tests. `bench/`: per-pattern benchmarks.

Docs: [`doc/ARCHITECTURE.md`](doc/ARCHITECTURE.md) (components, task tree,
data flow), [`doc/GETTING_STARTED.md`](doc/GETTING_STARTED.md) (ZeroMQ crash
course), `doc/socket-types/` (draft types), [`doc/RELEASE.md`](doc/RELEASE.md)
(tags and publishing).

## Commands

```sh
export OMQ_DEV=1 OMQ_RS_PATH=../omq.rs/bindings/ruby

bundle exec rake                 # all gem suites
bundle exec rake test:omq        # core only
bundle exec ruby -Ilib:test test/omq/heartbeat_test.rb -v
bundle exec ruby --yjit bench/push_pull/omq.rb
bundle exec ruby bench/report.rb # compare the last two bench runs
```

Both variables point at sibling checkouts (`../nuckle`, `../lz4rip`,
`../zrip`, `../omq.rs`). Run suites with `-v` and a short timeout; a suite
takes seconds, a hang is a bug.

## Conventions

- Ruby >= 3.3, best on 4.0 with YJIT. Some gems need 4.0.
- `CHANGELOG.md`: extend `## [Unreleased]` only. Never edit a released
  section.
- Docs stay brief and technical. No walls of text, no change history,
  benchmark results or tuning constants in `doc/ARCHITECTURE.md`. Rewrite it
  when the architecture changes.
- Don't create new doc files unless asked. Update existing ones.
- Commit messages: title plus one or two short paragraphs, plain text.
- Draft socket types (`omq/client_server`, `omq/radio_dish`,
  `omq/scatter_gather`, `omq/channel`, `omq/peer`) are not loaded by
  `require "omq"`. The routing and transport registries freeze on first
  use, so plugins and draft types must be required up front. No `autoload`.

## Code style

- Align `=` in consecutive assignments. Long lines are fine.
- Full `def ... end`, no endless methods. Two blank lines between methods.
  Blank lines between logical groups inside a method. One statement per line.
- Constants first, then `attr_*`, then methods. Blank line before the
  closing `end` of a class.
- Multi-line blocks use `do ... end`. On hot paths prefer `{ it.foo }` to
  `&:foo`.
- No `case/when/then` one-liners. No rubocop disable comments. No anonymous
  modules: anything that ends up in `ancestors` has a name.
- Monotonic time is `Async::Clock.now`.
- In prose, instance methods are `#name` and microseconds are µs.

## Code quality

API design:

- Keep public APIs small and hard to misuse. Default to `private`. Don't
  leak engine or routing internals through `Socket`.
- Related knobs go in one `Data.define` config. Async outcomes return an
  `Async::Promise`, not a callback block.
- Document every public method with YARD (`@param`, `@return`, `@raise`).
- New errors subclass the closest standard one (`SocketClosedError <
  IOError`) so existing rescues keep working.

Code shape:

- One concern per file. Split a file past about 800 lines along its concerns.
- Methods stay short and take few arguments. Pass a named object when
  arguments travel together.
- Logic shared by socket types or transports lives in one module
  (`ClosableQueues`, `AcceptLoop`), not in copies. Fix every copy when a
  plugin gem duplicates core code.
- Off the hot path, plain readable code beats clever code.

Errors:

- Peer input never raises out of the engine and never crashes a socket. A
  misbehaving peer is disconnected. Check sizes and counts before
  allocating.
- Rescue narrowly (`CONNECTION_LOST`, `CONNECTION_FAILED`,
  `Protocol::ZMTP::Error`). No bare `rescue` around I/O.

Tests:

- minitest/spec (`describe`/`it`) with `assert_*`/`refute_*`.
- Write the regression test first and see it fail before fixing.
- Wait on events (`peer_connected`, `all_peers_gone`, monitor events), not
  on sleeps. Never add a timeout to paper over a race. Sleep only to let time
  pass when asserting that nothing happens.
- Sockets in tests set `linger = 0`, so `ensure` cleanup can't hang on
  unsent messages.

## Socket fundamentals

These bind every change. Reliability is not negotiable: never weaken
self-healing for convenience.

- Send and receive never fail because of peers. Only misuse, timeouts
  (`IO::TimeoutError`), `router_mandatory` or a closed socket raise. After
  close, `#receive` returns nil and `#send` raises `SocketClosedError`.
- Connect before bind works. Sockets reconnect with backoff; applications
  never manage connections.
- Heartbeats detect dead peers, not slow ones. A connection we stopped
  reading (full receive queue) never expires.
- Messages are atomic: delivered whole or not at all.
- HWM means backpressure. PUB, XPUB and RADIO drop for a mute subscriber
  (`on_mute`); ROUTER drops for a full peer unless `router_mandatory`; the
  rest block.
- Fast peers never starve slow ones, slow peers never block fast ones.
  Receives fair-queue across connections.
- Peers are untrusted. Bound everything a peer can fill: frame size, parts
  per message, subscriptions, handshake properties, queues.
- One socket binds and connects several transports at once. Inproc skips
  ZMTP and the kernel.
- libzmq behavior is the reference for wire and socket semantics. Diverge
  only on purpose and record it in `doc/ARCHITECTURE.md`.

19 socket types, compatibility checked in the handshake:

| Pair | Semantics |
| --- | --- |
| PUSH/PULL, SCATTER/GATHER | Work-stealing out, fair-queue in |
| PUB/SUB, XPUB/XSUB | Prefix subscriptions; X types expose them as messages (proxies) |
| RADIO/DISH | Exact groups (`join`/`leave`); also over `udp://` |
| REQ/REP | Strict alternation; REQ adds an empty delimiter, REP restores the envelope |
| DEALER/ROUTER | Free alternation; ROUTER prepends and routes by identity |
| CLIENT/SERVER | Single-part DEALER/ROUTER; SERVER routes by routing id |
| PAIR, CHANNEL | Exactly one peer |
| PEER | N:N, routing id in both directions |

CLIENT, SERVER, RADIO, DISH, SCATTER, GATHER, CHANNEL and PEER are draft
types and single-frame only.

## Concurrency

- All I/O runs on Async fibers. No threads inside a socket; `Reactor` hosts
  sockets used outside a reactor on one `omq-io` thread.
- Every task has an owner barrier: socket, dialer or connection
  (`doc/ARCHITECTURE.md`). Spawn transient tasks under the right one. Only
  `Reactor` opens a bare `Async {}`. Use `Async::Barrier` for fork-join and
  `Async::Semaphore` for bounded slots.
- A finished task in a connection barrier means the connection is dead (the
  supervisor tears it down). Helper fibers that may finish early go under
  their own parent task.
- No lost wakes: change state before signaling, recheck the condition after
  waking.
- `Async::Stop`/`Async::Cancel` can land at any blocking call. Wire writes
  finish at a frame boundary (`defer_cancel`). Rescue cancellation only at
  task boundaries.
- Bound every queue and buffer a peer can fill. Budget every drain loop by
  message count and bytes, then yield.

## Performance

Profile before optimizing. Change measured hotspots, and back claims with
before and after numbers (YJIT, benchmarks run one at a time).

- The message path allocates nothing it can avoid. Parts are frozen binary
  strings, passed through without copying.
- Fan-out encodes a message once and writes the same wire bytes to every
  unencrypted subscriber.
- Writes are batched per pump cycle and flushed once per connection.
- Command frames (READY, PING, SUBSCRIBE) are not the hot path. Keep them
  simple.
