# Changelog

## [Unreleased]

### Changed

- Renamed the gem from `omq-ffi` to `omq-backend-libzmq`.
- Added `require "omq/backend/libzmq"` and `backend: :libzmq`.
- Kept `require "omq/ffi"` and `backend: :ffi` as compatibility aliases.
- Raised the supported Ruby version to 4.0+.
- Moved release source to the `zeromq/omq.rb` monorepo.

## 0.3.1 — 2026-04-18

### Added

- **`Engine#on_io_thread?`** — exposes the existing `@on_io_thread`
  flag. Required by `omq ~> 0.24`, whose `Writable#send` and
  `Readable#receive` consult it to skip `Reactor.run` when the socket
  was created under an Async fiber. Harmless on earlier omq versions.


## 0.3.0 — 2026-04-17

### Changed

- **Require `omq ~> 0.23`.** Track the upstream API change where
  `Socket#bind` / `#connect` return the resolved `URI` instead of
  setting the dropped `#last_endpoint` / `#last_tcp_port`
  accessors. `Engine#bind` returns `URI.parse(resolved_endpoint)`
  so callers get the auto-selected TCP port via `uri.port`.
  `Engine#connect` returns `URI.parse(endpoint)`. The private
  `extract_tcp_port` helper is gone.

### Fixed

- **`Float::INFINITY` linger no longer crashes the I/O thread.**
  Upstream omq's default linger is now `Float::INFINITY`, which
  blew up `(linger * 1000).to_i` with `FloatDomainError: Infinity`
  inside the dedicated I/O thread both at socket setup and at
  close. Extracted `Engine.linger_to_zmq_ms` that maps `nil` /
  `Float::INFINITY` to libzmq's `-1` (infinite) and otherwise
  converts seconds → milliseconds.

## 0.2.0 — 2026-04-10

### Changed

- **`Engine#dequeue_recv_batch` replaced with `Engine#dequeue_recv`.**
  Tracks the upstream omq change that drops the per-socket prefetch
  buffer in `Readable`. Each receive now dequeues a single message.

### Fixed

- **`Engine#capture_parent_task` accepts a `parent:` kwarg** to match
  the pure-Ruby `Engine` contract. The pure-Ruby side grew a
  `parent:` kwarg on `Socket#bind`/`#connect` (omq 0.16.3) and passes
  it through; FFI's implementation had no such parameter and raised
  `ArgumentError: unknown keyword: :parent` on the first bind/connect
  from inside an Async task.

## 0.1.3 — 2026-04-09

### Added

- **`Engine#parent_task`, `#monitor_queue=`, `#verbose_monitor=` stubs.**
  `Socket#monitor` from the pure-Ruby side was crashing with
  `NoMethodError: undefined method 'monitor_queue=' for an instance of
  OMQ::FFI::Engine` when used with `backend: :ffi`. The writers are
  no-ops for now (libzmq's `zmq_socket_monitor` wiring is still TODO),
  but `Socket#monitor` attaches cleanly instead of raising.

### Fixed

- **Map libzmq errno to the proper `Errno::*` subclass.** Bind/connect
  failures used to raise a plain `RuntimeError` with the strerror
  message as text, while the pure-Ruby backend raised real
  `Errno::EADDRINUSE` / `Errno::ECONNREFUSED` / `Errno::ENOENT`. Callers
  that rescued by class (e.g. the CLI's retry logic) silently missed
  FFI-backed failures. FFI errors now go through a new `syscall_error`
  helper that constructs a `SystemCallError` from `zmq_errno`, yielding
  the same `Errno::X` subclass the pure-Ruby backend would raise.

- **Linger-aware drain on close** — `Engine#close` used to hard-cap the
  I/O thread join at 2 seconds regardless of `options.linger`, and the
  `io_loop` ensure block only did a single non-blocking `drain_sends`
  pass. On a HWM-full socket or a large burst, pending messages were
  dropped even with `linger: nil` ("wait forever"). Now the ensure block
  retries `drain_sends` with `IO.select` on the ZMQ fd until the
  Ruby-side queue is empty or a monotonic `Async::Clock.now`-based
  deadline is hit, then re-applies the current `options.linger` to
  `ZMQ_LINGER` so `zmq_close` honors user changes made after socket
  creation. `close` joins the I/O thread with `nil` (infinite) / `0.5s`
  (linger=0) / `linger + 1s` (finite) instead of a fixed 2s cap, with a
  safety `kill` if the deadline still expires. Surfaced in a cross-tool
  PUSH/PULL benchmark where 500k messages of 100 B would leave the
  receiver hanging because the sender exited with messages still queued.
- **Nil linger crash** — `apply_options` did `@options.linger * 1000`,
  which raised `NoMethodError` when `linger` was `nil` (the "wait
  forever" sentinel from omq). Now maps `nil → -1` (libzmq's infinite).

## 0.1.2 — 2026-04-08

### Fixed

- **Lost messages on send-then-close** — `io_loop` used to break out on
  `:stop` before draining the Ruby-side `@send_queue`, so any message
  enqueued just before `close` was dropped instead of being handed to
  libzmq for `LINGER` to flush. `drain_sends` is now called once more in
  the loop's `ensure` block, so pending messages reach libzmq before
  `zmq_close` runs and `LINGER` can do its job. Surfaced when running
  short CLI commands like `omq push --ffi -n 1 -E '"hi"' -c tcp://...`.

## 0.1.1 — 2026-04-07

- YARD documentation on all public methods and classes.
- Code style: two blank lines between methods and constants.

## 0.1.0

Initial release.
