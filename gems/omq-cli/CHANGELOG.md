# Changelog

## 0.19.0 — 2026-08-03

- **Rust backend selection via `--backend rust`.** Loads
  `omq-backend-rust` and passes `backend: :rust` to socket
  constructors.
- **Monorepo import.** `omq-cli` now lives under `gems/omq-cli` in
  `omq.rb` and participates in monorepo test and release tasks.

## 0.17.1 — 2026-05-04

- bump deps

## 0.17.0 — 2026-04-25

- **LZ4 compression via `--lz4`.** Complements Zstd with a
  lower-CPU, lower-memory codec (no entropy stage, ~16 KiB of
  encoder state per connection). Uses the `lz4+tcp://` transport
  from `omq-lz4`. The existing `--compress` flag now accepts a
  codec-aware `SPEC`:
  ```
  --compress=zstd            # zstd at default level
  --compress=zstd:3          # zstd level 3
  --compress=zstd:-1         # zstd level -1
  --compress=lz4             # lz4 (no level)
  --compress=19              # back-compat: zstd level 19
  ```
  `Config.compress` becomes a tri-state symbol
  (`nil` / `:zstd` / `:lz4`); `SocketSetup.compress_url` picks
  `zstd+tcp://` or `lz4+tcp://` accordingly. Adds `omq-lz4` as a
  runtime dependency.
- **`omq pipe`: `-z` / `-Z` / `--compress` are modal with
  `--in` / `--out`.** Placing a compression flag after `--in`
  compresses only the PULL side; after `--out` only the PUSH
  side. A flag before any `--in`/`--out` still applies globally
  (backwards compatible). New `Config` fields
  `in_compress` / `in_compress_level` /
  `out_compress` / `out_compress_level` carry the per-side
  state; `SocketSetup.resolve_compress(config, side)` is the
  single resolution point, and `SocketSetup.attach_endpoints`
  takes a `side:` kwarg.
- **Bind-mode senders with piped stdin now wait for a peer.**
  `seq 10 | omq push -b tcp://…` previously bound, drained all
  messages into HWM, then exited before any consumer connected.
  Non-tty stdin now counts as a bounded send plan in
  `BaseRunner#bounded_or_scheduled_send?`, matching the existing
  behaviour for `-d` / `-F` / `-E`. Interactive-tty senders still
  go through unwaited so typing isn't gated on a peer.

## 0.16.4 — 2026-04-20

- **Use omq ~> v0.26.2

## 0.16.3 — 2026-04-20

- **Use omq ~> v0.26.1

## 0.16.2 — 2026-04-18

- **Use omq ~> v0.24.0 and omq-ffi >= v0.3.1**

## 0.16.1 — 2026-04-18

- **Use zstd ~> v0.4.0**

## 0.16.0 — 2026-04-18

- **Compression: switch from `OMQ::Compression::Zstd` helpers to the
  `zstd+tcp://` transport.** `-z` now upgrades `tcp://` URLs to
  `zstd+tcp://` and passes compression kwargs (`level:`) at bind /
  connect time. Pipe runners and parallel workers take `config:` to
  thread the flag through. Removes the passive/active distinction
  and the pure-send opt-out list — the transport handles framing.
- **`SocketSetup.attach` / `#attach_endpoints` use URI return values
  from `Socket#bind`.** Drops reliance on the dropped
  `Socket#last_endpoint` accessor (omq 0.12 change). Tests capture
  the resolved URI from `#bind` directly.
- **Dependencies consolidated.** Drops the five `omq-rfc-*` socket-type
  gems (`clientserver`, `radiodish`, `scattergather`, `channel`, `p2p`)
  — those socket types are now bundled with `omq` itself and opted
  into via `require "omq/<name>"`. Renames `omq-rfc-zstd` → `omq-zstd`
  and switches the require from `omq/rfc/zstd` to `omq/zstd`. Bumps
  `omq` to `~> 0.23` and `omq-ffi` to `~> 0.3`.

### Added

- **Formatter preview: per-frame byte budget scales with part count.**
  Single-part messages show 40 bytes; 2-part show 40 each; 3+ parts
  show 12 bytes per frame (the previous fixed limit).

## 0.15.3 — 2026-04-16

### Changed

- **Cache decoded `--data` input.** `read_inline_data` and
  `ParallelWorker#compute_reply` now memoize the decoded result
  instead of re-decoding the same literal on every iteration.

### Added

- **REQ generator mode (`-E`/`-e` with no stdin).** `omq req` now
  produces each request from the send-eval alone when no `-d`/`-f`
  is given and stdin is not piped, matching the existing PUSH/PUB
  behaviour. Bounded by `-n` or paced by `-i` like the other
  generator-capable runners.

### Tests

- **System tests for REQ and PUB `-E` generator mode.** REQ fires
  `-E'"foo"' -n 3` against a REP running `-e '|(a)| a.upcase' -n 3`
  and verifies three `FOO` replies round-trip. PUB fires
  `-E'"tick"' -i 0.05 -n 3` against a SUB and verifies three `tick`
  frames are received.
- **System tests split into themed files under `test/system/`.** The
  monolithic `test/system_test.sh` is replaced by 12 standalone files
  (req_rep, push_pull, pub_sub, router_dealer, format, compression,
  interval, transport, script, validation, pipe, parallel) sharing
  `support.sh` helpers. `run_all.sh` chains them; each file also runs
  standalone. `rake test:system` now invokes `run_all.sh`.

## 0.15.2 — 2026-04-15

### Fixed

- **`-vvv` trace output for REQ/REP/PAIR.** `recv_msg` now calls
  `trace_recv` itself, so every runner logs received messages under
  `-vvv` (previously only `run_recv_logic` / `recv_tick` runners did,
  leaving REQ/REP/PAIR receive-side traces silent).

### Tests

- **System test for REQ/REP verbose trace order.** Asserts REQ logs
  `>> hi` then `<< HI`, and REP (with `-e'it.first.upcase'`) logs
  `<< hi` then `>> HI`.
- **Test suite aborts on background-thread exceptions.**
  `Thread.abort_on_exception = true` in `test_helper.rb` — a raising
  spawned thread now re-raises in the main thread immediately instead
  of leaving the test hanging on a receive from a dead peer.
- **`connect_before_bind_test`**: drop `linger: 1` from
  `OMQ::PULL.new` — PULL is read-only and doesn't accept `linger`.

## 0.15.0 — 2026-04-14

### Changed

- Use omq-rfc-zstd ~> 0.2

## 0.14.9 — 2026-04-14

### Fixed

- **Bind-mode one-shot senders now wait for a peer before firing.**
  `omq push -b tcp://:5050 -ME '"foo"*3'` used to bind, queue the
  message into HWM, close the socket, and exit — frequently before
  any PULL had even connected, surfacing as a mysterious
  linger-timeout on exit. `needs_peer_wait?` now also returns true
  for bind-mode senders whose send plan is bounded or scheduled
  (`-d` / `-f` / `-E` / `-i`), so the runner blocks on
  `peer_connected.wait` before sending and then drains cleanly via
  linger on close. Interactive stdin is unchanged — typing isn't
  gated on a peer.
- **`--count N` now works with `-d` / `-f` / `-E` one-shot sends.**
  Previously only `-i` / stdin respected `--count`; a pure-generator
  run fired exactly once regardless. The `-d`/`-f` and `-E` branches
  in `run_send_logic` now loop `n` times (default 1).
- **`^C` while paging `--examples` is quiet.** Hitting ^C in the
  pager used to leak an `Interrupt` stack trace from `IO.popen`;
  `CLI.page` now rescues `Interrupt` alongside `Errno::EPIPE`.

### Changed

- **`--examples` output is ASCII-only and tightened up.** Replaced
  the two em-dashes in the Marshal section with ASCII parentheses,
  added a note to the Pipe section explaining that `@work` /
  `@sink` are Linux abstract-namespace unix sockets
  (`ipc://@name`), Linux-only, and to fall back to `ipc:///tmp/…`
  on macOS/BSD. Marshal and Compression prose trimmed from
  paragraph form to 2-3 line summaries.

## 0.14.8 — 2026-04-14

### Changed

- **`BaseRunner#log` lines now carry the `omq:` prefix and honor
  `--timestamps`.** `log "Peer connected"` used to emit a bare
  `Peer connected` to stderr, while every other stderr line went
  through `Term` and rendered as `omq: connecting to …`,
  `omq: disconnected …`, etc. Mixing the two was jarring — a
  single `omq push` run produced both `omq: connecting to …` and
  `Peer connected` back-to-back. `#log` now routes through
  `Term.log_prefix` and emits `{prefix}omq: {msg}`, and the three
  existing messages are lowercased to match the rest of the
  stderr vocabulary (`peer connected`, `subscriber joined`,
  `all peers disconnected, exiting`).

## 0.14.7 — 2026-04-14

### Fixed

- **v0.14.6 regression: `-e`/`-E` pure-generator mode broke on a
  TTY.** The new `config.stdin_is_tty` fallback was inserted into
  the same `elsif` as `stdin_ready?`, so a bare `omq push -E '"foo"'`
  on a terminal routed into `run_stdin_send` instead of firing the
  generator once. `run_send_logic` now checks `@send_eval_proc`
  before the TTY fallback, and the fallback is only taken when no
  other send source is configured. Surfaced by the
  "marshal send path: PushRunner sends raw object" trace-order
  test in CI.

## 0.14.6 — 2026-04-14

### Fixed

- **Bare `omq push -c tcp://…` on a terminal no longer exits
  immediately.** `BaseRunner#run_send_logic` only fell through to
  `run_stdin_send` when `stdin_ready?` was true, and `stdin_ready?`
  hard-codes `false` on a tty — so `omq push` / `omq pub` /
  `omq scatter` / `omq radio` / `omq pair` with no `-d` / `-f` /
  `-e` / `-I` connected, sent nothing, and disconnected. The
  elsif now also matches `config.stdin_is_tty`, so an interactive
  run reads lines from the terminal the way `omq req` and
  `omq rep` already did.

## 0.14.5 — 2026-04-14

### Fixed

- **Blank stdin lines are now a no-op instead of a zero-frame
  "message".** 0.14.3 decoded a blank line to `[""]` so that REQ
  would actually send *something* and not wedge in `recv_msg`.
  That was the wrong shape of fix — on a tty, hitting Enter on an
  empty prompt should behave like a shell's empty prompt and just
  wait for the next line, not fire off an empty ZMTP frame to the
  peer. `BaseRunner#read_stdin_input` now loops past blank lines
  for the ascii/quoted/jsonl paths; decode goes back to returning
  `[]` for a blank line and `Formatter::EMPTY_MSG` is gone. REQ
  still cannot wedge because the blank line is never seen by
  `run_loop` at all. `split("\t", -1)` / trailing-empty-frame
  preservation is kept.

## 0.14.4 — 2026-04-14

### Fixed

- **`NameError` on load when requiring `omq/cli/formatter` without
  `protocol/zmtp` already loaded.** 0.14.3 introduced
  `Formatter::EMPTY_MSG = [::Protocol::ZMTP::Codec::EMPTY_BINARY]`
  at class-body load time, which blew up with `uninitialized
  constant Protocol` in the release gem.

## 0.14.3 — 2026-04-14

### Fixed

- **`omq req` (and other interactive senders) no longer wedge on
  blank input lines.** `Formatter#decode` used to return `[]` for
  a blank line, which `BaseRunner#send_msg` then silently dropped
  — so REQ never sent a request but still blocked in `recv_msg`
  waiting for a reply. Blank lines now decode to a single empty
  frame (`[""]`) via the new
  `Formatter::EMPTY_MSG = [Protocol::ZMTP::Codec::EMPTY_BINARY]`
  constant, so the request actually goes out. As a side effect,
  ascii/quoted decoding now uses `split("\t", -1)` and preserves
  trailing empty frames (`"a\t\n"` → `["a", ""]`).
- **Cleaner `:disconnected` log lines on plain peer close.**
  `-vv` used to emit `omq: disconnected tcp://… (Stream finished
  before reading enough data!)` — the raw io-stream message for
  what is really just an `EOFError` at the ZMTP framing boundary.
  `Term.format_event` now routes through a new
  `Term.format_event_detail` helper that rewrites `EOFError` to
  `(closed by peer)`, leaving other errors' messages untouched.
  The underlying `event.detail[:error]` is unchanged.

### Changed

- **Lowercased `Term.format_attach` verbs.** `omq: Bound to …` /
  `omq: Connecting to …` now render as `omq: bound to …` /
  `omq: connecting to …`, matching the lowercase style already
  used by every other `omq:` log line (`disconnected`, `listening`,
  `handshake_succeeded`, …).

## 0.14.2 — 2026-04-13

### Changed

- `kill_on_protocol_error` is now a single
  `SocketSetup.kill_on_protocol_error(sock, event)` class method.
  Previously `BaseRunner`, `ParallelWorker`, and `PipeRunner` each
  carried an identical 4-line copy of the CLI policy that
  protocol-level disconnects mark the socket dead.
- `ExpressionEvaluator.extract_block` is now a single class method
  used by both the instance compile path and the
  `compile_inside_ractor` path. The in-Ractor copy previously lived
  as a local lambda that duplicated the instance
  `extract_block` method.
- `Formatter#encode` drops one String allocation per message on
  the ascii / quoted / jsonl / marshal paths by mutating the
  fresh `.join` / `JSON.generate` / `.inspect` result with `<<`
  instead of `+ "\n"`.
- `Formatter.marshal_preview` and `Formatter.frames_preview` (extracted
  from `Formatter.preview` in the `-vvv` marshal trace work) are now
  `private_class_method` — they were only ever meant to be called
  through `Formatter.preview` but ended up on the public class surface.
- Dropped a redundant unary `+` before `Formatter.sanitize(...)` in
  `marshal_preview`: `sanitize` already returns a fresh mutable String
  via `.tr`, so the `+""` dup was dead weight.
- **`-vvv` marshal trace headers now show plaintext and wire byte
  sizes.** Previously `<< (marshal) ...` carried no size info; it
  now renders as `(135B marshal) ...` and, when ZMTP-Zstd
  compression is negotiated, `(135B wire=50B marshal) ...` —
  matching the frame-based preview format used by every other
  `-vvv` output.
  Other formats (ascii/quoted/jsonl/msgpack/raw) already showed
  plaintext size via the frame preview; they now also pick up
  `wire=NB` when compression is active, since `wire_size` is
  side-channelled from `:message_sent` / `:message_received`
  monitor events. Send-side `wire_size` is best-effort — the
  engine's send pump emits the compressed byte count
  asynchronously, so the value reflects the most recently
  *completed* send; receive-side is exact.
- Hot-path optimized.

## 0.14.1 — 2026-04-13

### Changed

- **`-M` (Marshal) now carries raw Ruby objects, not array-wrapped
  frames.** Under `-M`, each wire frame is one Marshal-dumped Ruby
  object; inside `-e` / `-E`, `it` is that object directly (not
  `[object]`). Enables natural scalar/hash/custom-class flows:

  ```sh
  omq push -b tcp://:5557 -ME '"foo"'
  omq pull -c tcp://:5557 -M -e '{it => it.encoding}'
  # => {"foo" => #<Encoding:UTF-8>}
  ```

  The previous one-element-Array wrap was cosmetic — it always
  produced exactly one wire frame anyway — so no multipart
  semantics are lost.

### Fixed

- **`-vvv` trace lines now precede stdout side-effects from
  `-e` / `-E`.** A `<<` / `>>` line is emitted from the app fiber
  *before* the eval expression runs, so sequences like
  `-e 'p it'` read strictly as `trace → eval output → body` on a
  shared tty. Previous design emitted traces from the monitor
  fiber and raced with stdout.
- **`-vvv` under `-M` now shows the app-level object, not wire
  bytes.** Preview header switches to `(marshal) <inspect>` with
  sanitization and 60-byte truncation, e.g.
  `<< (marshal) [nil, :foo, "bar"]`.
- **`-vvv` trace preview sanitizes control characters.** Tabs,
  newlines, CR, and backslash render as `\t`, `\n`, `\r`, `\\`;
  other non-printables collapse to `.`. Previously raw LF inside
  a binary frame could leak and break the single-line guarantee.
- Test suite runs cleanly without protocol-error stderr noise.

## 0.14.0 — 2026-04-13

### Added

- **Receive-capable sockets decompress by default.** All socket types
  except pure senders (`push`, `pub`, `scatter`, `radio`) now advertise
  the ZMTP-Zstd profile in **passive mode** at startup, so they accept
  compressed frames from any active-sender peer without requiring
  `-z` on the receive side. They never compress their own outgoing
  frames in this mode — use `-z` / `-Z` / `--compress=LEVEL` on the
  sender to opt it in. A `push` piped into a `pull` with no flags on
  either side stays uncompressed; `omq push -z | omq pull` compresses
  on the wire and the pull side decodes transparently. This is the
  RFC Sec. 6.4 "Passive senders" mode; requires omq-rfc-zstd >= 0.1.0.
- **`-Z` flag for better-ratio compression (zstd level 3).** `-z`
  remains the fast default (level -3) and `--compress=LEVEL` takes
  a custom zstd level (e.g. `--compress=19`, `--compress=-1`). Short
  bundling (`-zvvv`, `-Zvvv`) still works.
- **`-vvv` logs `ZDICT` exchange.** When the auto-trained dictionary
  is shipped/received, the trace prints `>> ZDICT (NB)` on the sender
  and `<< ZDICT (NB)` on the receiver.
- **`-vvv` wire-size annotation for compressed traces.** Message
  previews on compressed sockets include the post-compression byte
  count: `(280B wire=29B) ZZ…`. Plumbed from the ZMTP-Zstd wrapper
  through the engine's verbose monitor.

### Changed

- **Compression backend switched from `rlz4` to `omq-rfc-zstd`.**
  Compression is now a ZMTP wire-protocol extension negotiated via
  the `X-Compression` READY property and applied below the
  application API. Auto-trained dictionaries are shipped over a
  `ZDICT` command frame once the sender has enough samples. The
  `Formatter` no longer compresses or decompresses anything — it
  only encodes/decodes wire formats. Pipe `-z` is no longer modal
  (`compress_in`/`compress_out` removed) since compression is a
  per-socket, send-side property negotiated with each peer.
- **`-vvv` output ordering under compression.** At `-vvv`, the
  monitor fiber now writes both the trace line and the plaintext
  body, so trace-and-body pairs land on the tty in order instead of
  interleaving between the recv pump and the app fiber.
- **TCP host normalization moved into `OMQ::Transport::TCP`.** `omq`
  v0.19.0 now handles `tcp://*:PORT`, `tcp://:PORT`, and
  `tcp://localhost:PORT` natively (including dual-stack `*` binding
  both IPv4 and IPv6 wildcards), so `CliParser` no longer rewrites
  these URLs before handing them off. Removed
  `CliParser.loopback_bind_host` and the `normalize_bind`/
  `normalize_connect`/`normalize_ep` block. Requires `omq ~> 0.19`.
- **Terminate on protocol errors instead of silent reconnect.** When
  a peer sends a frame that violates the ZMTP wire protocol
  (oversized, bad framing, zstd bytebomb, nonce exhaustion, …), the
  library drops that one connection and reconnects — the libzmq
  parity behavior. The CLI is a different audience: a persistent
  protocol violation is almost always a misconfiguration the user
  needs to see, not silently paper over. Every runner
  (`BaseRunner`, `PipeRunner`, `ParallelWorker`, `PipeWorker`) now
  attaches a monitor that watches for `:disconnected` events whose
  `detail[:error]` is a `Protocol::ZMTP::Error`, prints
  `omq: <reason>` to stderr, kills the socket, and exits with
  status 1. Requires `omq ~> 0.19.2` for the new `:disconnected`
  detail shape and `Socket#engine` accessor.
- **`-vvv` disconnect events render the reason in parentheses.**
  `Term.format_event` now pretty-prints `:disconnected` details
  that contain a `:reason` key, e.g.
  `disconnected tcp://:5555 (frame size 1024 exceeds max_message_size 32)`,
  instead of dumping the raw hash.

### Fixed

- **`--recv-maxsz` is now actually applied in pipe and parallel
  modes.** `PipeRunner`, `PipeWorker`, and `ParallelWorker` were
  only calling `SocketSetup.apply_options` + `apply_compression`
  and skipping `max_message_size` entirely — so the default 1 MiB
  cap (and any `--recv-maxsz` override) silently had no effect on
  `omq pipe` or `omq pull -P`. Extracted the logic into
  `SocketSetup.apply_recv_maxsz` and wired it into all four setup
  paths (sequential pull/rep, sequential pipe, parallel worker,
  pipe worker). Oversized frames now drop the connection as
  intended and — combined with the CLI termination policy above —
  exit with a clear error instead of hanging in a reconnect loop.

## 0.13.0 — 2026-04-12

### Added

- **`--timestamps[=PRECISION]` flag.** Prefix log lines with UTC
  timestamps. Accepts `s`, `ms` (default), or `us`. Replaces the
  former `-vvvv` special meaning, which has been removed.
- **`-M` / `--marshal` preserves arbitrary objects on the wire.**
  Eval results in `--format marshal` mode (e.g. `Time.now`, hashes,
  UTF-16LE strings) are now passed through unchanged instead of
  being coerced via `#to_s`. Affects both the main runner and
  Ractor workers (`-P`).
- **`-P0` ⇒ `nproc`.** `-P0` (or bare `-P`) spawns one Ractor worker
  per CPU, capped at 16. Short-option clustering works: `-P0zvv`
  expands to `-P 0 -z -v -v`.

### Changed

- **`-vvv` shows decompressed message previews.** When `--compress`
  is active, message traces now log the decompressed parts and
  include the on-wire size: `(5B wire=21B) hello`. Engine-level
  message events are suppressed in compressed mode to avoid
  double-logging compressed bytes.
- **Truncation marker uses `…` (U+2026).** `Formatter.preview`
  truncation now uses the real horizontal ellipsis character
  instead of three ASCII dots.

### Fixed

- **`FrozenError` in eval handlers returning received parts.**
  `ExpressionEvaluator` no longer mutates the result array with
  `#map!`, which crashed when a script handler returned the frozen
  parts array received from the socket.
- **README:** fixed broken `if /regex/` examples (no implicit `$_`
  match in `instance_exec`), broken `OMQ.incoming`/`OMQ.outgoing`
  handler table, and `-P` examples that no longer parsed.

## 0.12.3 — 2026-04-10

### Fixed

- Gem version

## 0.12.2 — 2026-04-10

### Fixed

- **Eval results coerced via `#to_s`.** Non-string eval results (e.g.
  `-E 'Time.now'`, `-E '[42, :sym]'`) are now coerced to strings
  instead of raising `NoMethodError` on `#to_str`. Array elements are
  coerced individually.

## 0.12.1 — 2026-04-10

### Changed

- **`it` replaces `$F` and `$_` in eval expressions.** The `-e`/`-E`
  message parts variable is now Ruby's default block variable `it`
  instead of the `$F` global. `$_` is removed — use `it.first` instead.
  This also simplifies Ractor worker compilation by removing the
  `$F` → `__F` rewrite.

- **Block parameter syntax in `-e`/`-E` expressions.** Expressions can
  now declare parameters like Ruby blocks: `-e '|msg| msg.map(&:upcase)'`.
  A single parameter receives the whole parts array. Use `|(a, b)|` for
  destructuring.

### Added

- **`@name` endpoint shorthand.** `-c@work` and `-b@sink` expand to
  `ipc://@work` and `ipc://@sink` (Linux abstract namespace). Only
  triggers when the value starts with `@` and has no `://` scheme.

### Fixed

- **Flaky FFI backend tests.** Wait for both REP peers to be connected
  before round-robining requests, instead of only waiting for the first.

- **Improved verbose preview format.** Empty frames render as `''`
  instead of `[0B]`. Multipart messages show frame count: `(18B 4F)`.

- **Compression skips nil/empty frames.** `compress` passes `nil` parts
  through (coerced to empty frames by the socket layer). `decompress`
  skips empty frames instead of feeding them to LZ4.

- **Pipe `--out` without `--in` promotes bare endpoints.** Bare `-c`/`-b`
  before `--out` are now treated as `--in` endpoints (and vice versa),
  fixing `pipe -c SRC --out -c DST` which previously errored.

- **Pipe fan-out distributes across output peers.** Multi-output pipes
  now yield after each send, giving send-pump fibers a turn to drain the
  shared queue. Without this, one pump monopolized the queue via
  `drain_send_queue_capped` when messages arrived in bursts.

- **Pipe waits for all output peers with `--timeout`.** `wait_for_peers_with_timeout`
  now waits for `connection_count >= out_eps.size` instead of just the first peer.

## 0.11.4 — 2026-04-10

### Fixed

- **Consistent `omq:` prefix on attach lines.** "Bound to" and
  "Connecting to" log lines now include the `omq: ` prefix, matching
  the format of monitor event lines.

## 0.11.2 — 2026-04-10

### Fixed

- **Endpoint normalization for `tcp://:PORT`.** Connects now normalize
  to `tcp://localhost:PORT` (preserving Happy Eyeballs) instead of
  `tcp://127.0.0.1:PORT`. Binds normalize to the loopback address
  (`[::1]` on IPv6-capable hosts, `127.0.0.1` otherwise) instead of
  `0.0.0.0` (all interfaces). `tcp://*:PORT` binds still expand to
  `0.0.0.0`. Explicit addresses (`0.0.0.0`, `[::]`, `127.0.0.1`) pass
  through unchanged. The macOS hang (IPv6 `connect(2)` stalling via
  kqueue) is fixed by the connect timeout in omq v0.17.3.

## 0.11.0 — 2026-04-10

### Added

- **`-vvvv` adds ISO8601 µs-precision timestamps** to every log line
  (endpoint attach, monitor events, message traces). Useful for
  debugging time-sensitive reconnect and handshake races where
  untimestamped `-vvv` output makes every event look instantaneous.
  `-v`/`-vv`/`-vvv` are unchanged.

- **YJIT enabled by default in `exe/omq`.** `RubyVM::YJIT.enable` is
  called before loading the CLI. Skipped if `RUBYOPT` is set (so users
  who pass `--disable-yjit` or similar keep their choice), if the
  interpreter lacks YJIT, or if YJIT is already on.

### Changed

- **Universal default HWM is now 64** (was 100 for most sockets, 16
  for pipe). 64 matches the recv pump's per-fairness-batch limit
  (one batch exactly fills a full queue). Removed the special-case
  `PipeRunner::PIPE_HWM = 16` override — pipe sockets now use the
  same default as everything else, eliminating the documented-but-
  surprising cliff.

- **`pipe` no longer waits for peers unless `--timeout` is set.** The
  previous unconditional `Barrier { pull.peer_connected + push.peer_connected }`
  gate served no correctness purpose: `PULL#receive` blocks naturally
  when no source is connected, and `PUSH` buffers up to `send_hwm` when
  no sink is connected, so the loop can start immediately. Concretely,
  `omq pipe -c ipc://@src -b ipc://@sink` started without a sink now
  drains the source until both sides' HWMs are full (recv_queue +
  send_queue = 2 × HWM) instead of silently blocking at 1 × HWM in the
  recv pump while the worker loop sat idle at the wait. When
  `--timeout` *is* set, the wait is preserved as a fail-fast starting
  gate.

- **Event formatting consolidated into `OMQ::CLI::Term`** — a new
  stateless module (`module_function`) with `format_attach`,
  `format_event`, `write_attach`, `write_event`, and `log_prefix`.
  Replaces four duplicated copies across `BaseRunner`, `PipeRunner`,
  `ParallelWorker`, and `PipeWorker` that had drifted apart — pipe-
  mode event lines were missing the `-vvvv` timestamp prefix because
  `PipeRunner` had its own copy of the formatter.

## 0.10.0 — 2026-04-09

### Changed

- **`--send-hwm` / `--recv-hwm` collapsed into a single `--hwm N`** option.
  Outside pipe modal mode it sets both send and recv HWM on the socket.
  Inside pipe modal mode it follows the current `--in` / `--out` side:
  `--in --hwm N` sets the input PULL's recv HWM, `--out --hwm N` sets
  the output PUSH's send HWM. Breaking CLI change — scripts must update
  from `--send-hwm`/`--recv-hwm` to `--hwm` (with `--in`/`--out` if they
  need per-side values in pipe).

## 0.9.0 — 2026-04-08

### Changed

- **`--recv-maxsz` defaults to 1 MiB in the CLI** — the underlying `omq`
  library no longer imposes a default (it's `nil`/unlimited as of this
  release), but the CLI keeps a conservative 1 MiB cap for safety when
  connecting to untrusted peers from a terminal. Pass `--recv-maxsz 0`
  to disable the cap explicitly, or `--recv-maxsz N` to raise it.
- **Default HWM lowered to 100** (from libzmq's 1000) for both send and
  recv. The CLI is typically used interactively or for short pipelines
  where a smaller in-flight queue keeps memory bounded and surfaces
  backpressure earlier. Users who want the old behavior can pass
  `--send-hwm 1000 --recv-hwm 1000` (or `0` for unbounded). Pipe worker
  sockets are unaffected — they still override to `PIPE_HWM` internally.
- **Compression codec: Zstandard → LZ4 frame format (BREAKING on the wire).**
  `--compress` now uses the new [`rlz4`](../rlz4) gem (Rust extension over
  `lz4_flex`) instead of `zstd-ruby`. Motivation: `zstd-ruby` is the only
  existing Ractor-safe compressor gem, but LZ4 is a better fit for the
  per-message-part workload (smaller frames, lower CPU). `rlz4` is
  Ractor-safe by construction, so parallel `-P` workers now use the same
  codec as the sequential path. **Wire format is incompatible** with prior
  omq-cli versions when `--compress` is in use — both ends must upgrade.

### Added

- **`--ffi` flag** — opt-in libzmq backend for any socket runner. Builds
  sockets with `backend: :ffi`, so the CLI can drive native libzmq instead
  of the pure-Ruby engine. Requires the optional `omq-ffi` gem and a
  system libzmq 4.x; missing dependencies abort with a clear error.
  Propagated through all socket construction sites: `BaseRunner`,
  `PipeRunner`, `PipeWorker`, and `ParallelWorker`.

### Fixed

- **`--send-eval` / `-E` on REP** — now rejected at validation time. REP
  derives its reply from `--recv-eval` / `-e`, so `-E` was silently
  ignored and the runner fell through to reading stdin, hanging the
  request-reply cycle.
- **`-vvv` preview of REP/REQ envelopes** — empty delimiter frames now
  render as `[0B]` instead of an empty string, so a REP reply with wire
  parts `["", "1"]` previews as `(1B) [0B]|1` instead of the misleading
  `(1B) |1` with a dangling leading pipe.

## 0.8.2 — 2026-04-08

### Fixed

- **`DecompressError` handling** — decompression failures now raise a proper
  `DecompressError` instead of calling `abort`. In parallel mode (`-P`),
  errors are sent through a dedicated `Ractor::Port` with a consumer thread
  that aborts cleanly (exit code 1, one error message). Previously, `Async do`
  swallowed the exception and the process exited silently.
- **Pipe memory growth** — pipe sockets now pass `recv_hwm` / `send_hwm` via
  constructor kwargs so the engine captures them before internal queue sizing.
  Previously, setters were called after the engine was initialized and had no
  effect on staging queue capacity.

### Changed

- **`-P N` mandatory argument** — `-P` now requires an explicit worker count.
  Previously, `-Pq` silently consumed `-q` as the argument to `-P`, making
  `-q` (quiet) ineffective.
- **Pipe default HWM lowered to 16** — reduced from 64 to further bound memory
  with large messages in pipeline stages.

## 0.8.1 — 2026-04-08

### Fixed

- **Binary frame preview** — compressed or binary message frames now show
  `[NB]` instead of unreadable dot-filled strings in `-vvv` monitor output.
  Frames where less than half the sample bytes are printable ASCII are detected
  as binary.

## 0.8.0 — 2026-04-08

### Added

- **`-P` for pull, gather, and rep** — parallel Ractor workers for recv-only
  and request-reply socket types. Output serialized through `Ractor::Port`
  to avoid scrambled stdout.
- **`RactorHelpers` module** — shared Ractor infrastructure: `preresolve_tcp`,
  `start_log_consumer`, `start_output_consumer` with `SHUTDOWN` sentinel for
  clean consumer shutdown.
- **`ParallelWorker` class** — general Ractor worker for parallel socket modes.
- **Process titles** — all runners set descriptive `proctitle`
  (`omq TYPE [-z] [-PN] ENDPOINTS`). Pipe shows `omq pipe [-z] [-PN] IN -> OUT`.
  Bare script mode shows `omq script`.

### Changed

- **ASCII-only source** — replaced all Unicode special characters (em-dashes,
  box-drawing, arrows) with ASCII equivalents in lib/ and test/.
- **Pipe default HWM** — pipe sockets now default to HWM of 64 (instead of
  the socket default 1000) to bound memory with large messages in pipeline
  stages. Override with `--send-hwm` / `--recv-hwm`.
- **Message preview** — total byte count first, 12 chars per part, max 3 parts
  shown (`(1234B) frame1|frame2|frame3|...(5 parts)`).
- **`SocketSetup.apply_options`** — extracted shared socket option setup,
  used by `BaseRunner`, `PipeRunner`, `PipeWorker`, and `ParallelWorker`.
- **`Formatter.preview`** — extracted from duplicated `msg_preview` methods.
- **Pipe/PipeWorker** — use bare `.new` for sockets, `SocketSetup.apply_options`
  for configuration, `RactorHelpers` for Ractor infrastructure.

## 0.7.2 — 2026-04-07

### Changed

- **Cleaned up pipe.rb** — removed unused `@fmt` formatters, dead `log` method,
  and `with_timeout` wrapper. Moved `preresolve_tcp` and `start_log_consumer` to
  `PipeWorker` class methods.
- **Guard `with_timeout(nil)`** — `Fiber.scheduler.with_timeout(nil)` fires
  immediately in Async; peer-wait and pipe sequential mode now skip the timeout
  wrapper when `config.timeout` is nil.

## 0.7.1 — 2026-04-07

### Fixed

- **Pipe `-P` preresolves TCP hostnames** — DNS resolution happens on the
  main thread before spawning Ractors, avoiding `Ractor::IsolationError`
  on `Resolv::DefaultResolver`. All resolved addresses (IPv4 + IPv6) are
  passed to workers.

## 0.7.0 — 2026-04-07

### Changed

- **`-P` restricted to pipe only** — parallel Ractor workers are no longer
  available on recv-only socket types (pull, sub, gather, dish). Pipe workers
  now use bare Ractors with their own Async reactors and OMQ sockets, removing
  the `omq-ractor` dependency entirely.
- **`-P` range capped to 1..16** — default is still `nproc`, clamped to 16.
  `-P 1` is valid (single Ractor worker, no sockets on main thread).
- **Removed `omq-ractor` dependency** — no longer needed.
- **Removed `ParallelRecvRunner`** — the Ractor bridge infrastructure for
  non-pipe socket types has been deleted.

### Fixed

- **Pipe `-P` END blocks execute after timeout** — worker recv loops now
  catch `IO::TimeoutError` so BEGIN/END expressions run to completion.

## 0.6.0 — 2026-04-07

### Added

- **Modal `--compress` for pipe `--in`/`--out`** — `--compress` after `--in`
  decompresses input, after `--out` compresses output. Enables mixed pipelines
  like plain input → compressed output:
  `omq pipe --in -c src --out --compress -c dst`.
  Without `--in`/`--out`, `--compress` applies to both directions as before.

### Fixed

- **Abort with clear message on decompression failure** — receiving an
  uncompressed message with `--compress` now prints a hint instead of
  silently killing the Async task with exit code 0.

## 0.5.4 — 2026-04-07

### Fixed

- **Fix `--compress` crash** — `Zstd` constant was uninitialized because
  `zstd-ruby` was no longer required at startup. Now required when `-z` is parsed.
- **Fix `--msgpack` crash** — same issue. Now required when `--msgpack` is parsed.

## 0.5.3 — 2026-04-07

### Fixed

- **HTTPS debug endpoint uses localhost.rb** — `OMQ_DEBUG_URI=https://...` now
  uses `Localhost::Authority` for self-signed TLS, fixing "no shared cipher"
  errors when accessing the async-debug web UI in a browser.

## 0.5.2 — 2026-04-07

### Fixed

- **Guard async-debug behind `OMQ_DEV`** — the Gemfile still caused the
  openssl conflict on CI even after removing it from the gemspec.

## 0.5.1 — 2026-04-07

### Fixed

- **Move async-debug from runtime to dev dependency** — async-debug depends on
  `openssl >= 3.0` which conflicts with Ruby's default openssl gem on CI.
  Now only loaded when `OMQ_DEBUG_URI` is set, with a `LoadError` guard and
  install hint.

## 0.5.0 — 2026-04-07

### Changed

- **rbnacl, zstd-ruby, and msgpack are now fixed dependencies** —
  no more runtime detection or conditional test guards.
- **`--curve-crypto` renamed to `--crypto`** — applies to CURVE and future
  mechanisms (e.g. BLAKE3ZMQ). Env var renamed from `OMQ_CURVE_CRYPTO` to
  `OMQ_CRYPTO`.
- **CURVE requires system libsodium** — rbnacl is bundled but needs libsodium
  installed (`apt install libsodium-dev` / `brew install libsodium`). nuckle
  (pure Ruby) is available via `--crypto nuckle` but marked as DANGEROUS.

### Removed

- **`has_zstd` / `has_msgpack` config fields** — no longer needed since both
  gems are fixed dependencies.

## 0.4.0 — 2026-04-07

### Added

- **`--sndbuf` / `--rcvbuf` options** — set `SO_SNDBUF` and `SO_RCVBUF` kernel
  buffer sizes. Accepts plain bytes or suffixed values (`4K`, `1M`).
- **Pipe FIFO ordering system test** — verifies sequential source batches are
  never interleaved through a pipe.
- **Pipe producer-first system test** — verifies messages are delivered when
  the producer finishes before the consumer connects.

### Changed

- **Message traces moved to monitor events** — `-vvv` traces now use
  `Socket#monitor(verbose: true)` instead of inline `trace_send`/`trace_recv`
  calls, ensuring correct ordering with connection lifecycle events.

### Fixed

- **Test helper `make_config`** — added missing `send_hwm`, `recv_hwm`,
  `sndbuf`, `rcvbuf` fields and changed `verbose` default from `false` to `0`.

## 0.3.1 — 2026-04-07

### Added

- **`--send-hwm` / `--recv-hwm` options** — set send and receive high water
  marks from the command line (default 1000, 0 = unbounded).
- **`OMQ_DEBUG` env var** — starts async-debug web UI on
  `https://localhost:5050` (or custom port via `OMQ_DEBUG=PORT`).
- **Multi-level verbosity** — `-v` prints endpoints, `-vv` logs all
  connection events (connect/disconnect/retry/timeout) via socket monitor,
  `-vvv` also traces first 10 bytes of every sent/received message.

### Fixed

- **`omq pipe` slow reconnection** — sequential `peer_connected.wait` calls
  blocked receiving until both PULL and PUSH peers connected in order. Now
  waits concurrently using `Kernel#Barrier`.
- **`-i` on recv-only sockets** — `pull -i 0.2` rate-limits receiving to
  one message every 200 ms using `Async::Loop.quantized`. Works on all
  recv-only socket types (pull, sub, gather, dish).
- **`--send-eval` with `-i` and piped stdin** — `seq 5 | omq push -E '…' -i 1`
  ignored stdin and produced no output. `#read_next_or_nil` now reads
  stdin when available; `#send_tick` distinguishes generator mode (no
  stdin, eval only) from stdin-exhausted EOF.

## 0.3.0 — 2026-04-07

### Fixed

- **Fix broken Gemfile** — remove deleted `omq-draft`, add all RFC gems
  with `OMQ_DEV` path resolution.
- **Fix CURVE API calls** — `Curve.server` and `Curve.client` now use
  keyword arguments to match the updated protocol-zmtp API.
- **Replace `require "omq/draft/all"`** with explicit RFC requires
  (`omq/rfc/clientserver`, `radiodish`, `scattergather`, `channel`, `p2p`).

### Changed

- YARD documentation on all public methods and classes.
- Code style: two blank lines between methods and constants.

### Refactored

- **`req_rep.rb` + `pair.rb` method extraction** — `ReqRunner#run_loop` (23 lines)
  gains `wait_for_interval`; `RepRunner#run_loop` (27 lines) gains
  `handle_rep_request`; `PairRunner#run_loop` (20 lines) gains `recv_async`.
  Each `run_loop` shrinks to ~10 lines.

- **`BaseRunner` method extraction** — `call` (23 lines), `wait_for_peer` (18),
  `read_next` (20), `run_send_logic` (29), and `compile_expr` (13) each
  decomposed into focused helpers: `setup_socket`, `maybe_start_transient_monitor`,
  `run_begin_blocks`, `run_end_blocks`, `wait_for_subscriber`, `apply_grace_period`,
  `read_inline_data`, `read_stdin_input`, `run_interval_send`, `run_stdin_send`,
  `compile_evaluator`, `assign_send_aliases`, `assign_recv_aliases`.
  Every public-facing method is now ≤10 lines.

- **`RoutingHelper`: extract `async_send_loop`, `interval_send_loop`, `stdin_send_loop`** —
  the sender `task.async` block was identical in `RouterRunner#run_loop` and
  `ServerRunner#monitor_loop`. Moved to `RoutingHelper` and split into three
  focused helpers. Both callers reduced to a 3-line orchestration.
  `ServerRunner#reply_loop` gained `handle_server_request` to hold the 3-branch
  dispatch, leaving the loop itself at ~8 lines.

- **`pipe.rb` method extraction** — `run_sequential` (50 lines) and `run_parallel`
  (105 lines) decomposed into focused helpers: `build_pull_push`,
  `apply_socket_intervals`, `setup_sequential_transient`, `sequential_message_loop`,
  `build_socket_pairs`, `wait_for_pairs`, `setup_parallel_transient`,
  `build_worker_data`, `spawn_workers`, `join_workers`. Each caller shrinks to
  ~8 lines.

- **Hoist `eval_proc` + `Integer#times` in Ractor worker loops** — in both
  `ParallelRecvRunner` and `pipe.rb#spawn_workers`, `if eval_proc` was checked
  on every message despite being invariant. Now a pre-loop branch splits into
  two (or four, combined with `n_count`) separate loops. Count limits use
  `n.times` instead of a manual `i` counter, eliminating the `n && n > 0`
  check from every iteration.

- **`ExpressionEvaluator.normalize_result`** — the `case result when nil/Array/String/else`
  block appeared in both Ractor worker bodies (`ParallelRecvRunner` and `pipe.rb`).
  Extracted to a class method and both callers updated to use it.

- **Extract `ParallelRecvRunner`** — `BaseRunner#run_parallel_recv` (107 lines
  of Ractor worker management) moved into `OMQ::CLI::ParallelRecvRunner` in
  `lib/omq/cli/parallel_recv_runner.rb`. Constructor takes `klass, config, fmt,
  output_fn`; `BaseRunner#run_parallel_recv` becomes a 4-line delegator.
  `BaseRunner` shrinks from 426 to ~325 lines.

- **Extract `CliParser`** — `parse_options` (178 lines), `validate!` (48 lines),
  `validate_gems!`, `DEFAULT_OPTS`, and the `EXAMPLES` heredoc (184 lines) moved
  from the `OMQ::CLI` module into a dedicated `OMQ::CLI::CliParser` class in
  `lib/omq/cli/cli_parser.rb`. `CLI.build_config` now calls `CliParser.parse`
  and `CliParser.validate!`; `CLI.run_socket` calls `CliParser.validate_gems!`.
  `cli.rb` shrinks from 674 to ~200 lines.

- **`ReqRunner#run_loop` deduplication** — the interval and non-interval
  branches were identical loops; merged into one with a trailing
  `sleep(wait)` gated on `config.interval`.
- **Remove empty runner subclasses** — `DealerRunner`, `ChannelRunner`,
  `ClientRunner`, and `PeerRunner` were empty class bodies that added no
  behaviour. `RUNNER_MAP` now points directly at `PairRunner`, `ReqRunner`,
  and `ServerRunner` for those socket types. `channel.rb` and `peer.rb`
  deleted; empty bodies removed from `router_dealer.rb` and
  `client_server.rb`.
- **Extract `TransientMonitor`** — `start_disconnect_monitor`,
  `transient_ready!`, and `@transient_barrier` removed from `BaseRunner`
  into `OMQ::CLI::TransientMonitor`. `BaseRunner` holds a single
  `@transient_monitor` collaborator; `transient_ready!` delegates to
  `monitor.ready!`.
- **Extract `RoutingHelper` module** — `display_routing_id`, `resolve_target`,
  and a `send_targeted_or_eval` template method (calling a `send_to_peer` hook)
  extracted from `RouterRunner` and `ServerRunner` into a shared module.
  `display_routing_id` and `resolve_target` removed from `BaseRunner`, which
  never used them directly.
- **Extract `SocketSetup`** — socket construction (`SocketSetup.build`),
  endpoint attachment (`SocketSetup.attach` for URL lists,
  `SocketSetup.attach_endpoints` for `Endpoint` objects), subscription/group
  setup (`SocketSetup.setup_subscriptions`), and CURVE configuration
  (`SocketSetup.setup_curve`) extracted from `BaseRunner` into a stateless
  module. `PipeRunner#attach_endpoints` now delegates to the shared module.
- **Extract `ExpressionEvaluator`** — expression compilation (`extract_block`,
  `extract_blocks`, BEGIN/END parsing, proc wrapping) and result normalisation
  live in `OMQ::CLI::ExpressionEvaluator`. Removes duplicate code from
  `BaseRunner`, `PipeRunner`, and both Ractor worker blocks. A shared
  `ExpressionEvaluator.compile_inside_ractor` class method replaces the
  identical inline parse lambdas that previously appeared in each Ractor block.

### Added (OMQ::Ractor integration)

- **`-P` extended to recv-only socket types** (`pull`, `sub`, `gather`,
  `dish`) when combined with `--recv-eval`. Each worker gets its own
  socket connecting to the external endpoint; ZMQ distributes work
  naturally. Results are collected via an inproc PULL back to main for
  output. Requires all endpoints to use `--connect`.
- **`omq-ractor` dependency** — `OMQ::Ractor` is now used for all
  parallel worker management.

### Changed

- **`pipe -P` rewritten using `OMQ::Ractor`** — workers no longer
  create their own Async reactor internally. Sockets are created and
  peer-waited in the main Async context, then passed to
  `OMQ::Ractor.new`; worker blocks contain only pure computation.
  Semantics are unchanged.

### Fixed

- **`-P` recv-only: stale round-robin entry** — the initial socket
  created by `call()` was connecting to the upstream endpoint before
  `run_parallel_recv` closed it, leaving a dead entry in the PUSH
  peer's round-robin cycle and silently dropping 1 in every N+1
  messages. Fixed by skipping `attach_endpoints` when `config.parallel`
  is set.
- **`-P` BEGIN/END blocks in Ractor workers** — `@ivar` expressions in
  BEGIN/END/eval blocks raised `Ractor::IsolationError` because `self`
  inside a Ractor is a shareable object. All three procs now execute via
  `instance_exec` on a per-worker `Object.new` context.
- **`-P` END block result forwarded** — the return value of an END block
  was discarded rather than forwarded to the output socket. Now captured
  and pushed to `push_p`, enabling patterns like
  `BEGIN{@s=0} @s+=Integer($F.first);nil END{[@s.to_s]}` to emit once
  per worker on exit.

---

### Added

- **`-r` defers loading to inside the Async reactor** — scripts now run
  within the event loop and may use async APIs, OMQ sockets, etc.
- **Bare script mode (`omq -r FILE`)** — omitting the socket type runs the
  script directly inside `Async{}` with free reign. The `OMQ` module is
  included into the top-level namespace so scripts can write `PUSH.new`
  instead of `OMQ::PUSH.new`.
- **`-r -` / `-r-`** — reads and evals the script from stdin, useful for
  quick copy-paste invocations. Cannot be combined with `-F -`.
- **`--recv-maxsz COUNT`** — sets the ZMQ `max_message_size` socket option;
  the socket discards incoming messages exceeding `COUNT` bytes and drops
  the peer connection before allocation.

### Fixed

- `Protocol::ZMTP::Codec::Frame` was referenced as bare `ZMTP::Codec::Frame`
  in the formatter (`--raw` format). Fixed the constant path.

## 0.2.0 — 2026-03-31

### Added

- **`omq keygen`** — generates a persistent CURVE keypair (Z85-encoded
  env vars). Supports `--curve-crypto rbnacl|nuckle` and the
  `OMQ_CURVE_CRYPTO` env var.
- **`--curve-crypto BACKEND`** — explicit crypto backend selection for
  CURVE encryption. Defaults to rbnacl if installed; no silent fallback
  to nuckle.
- **`-v` logs crypto backend** — `omq keygen -v` and socket commands
  with `-v` log which CURVE backend was loaded.

### Changed

- **CURVE uses protocol-zmtp** — replaces the old omq-curve gem with
  `Protocol::ZMTP::Mechanism::Curve` and a pluggable `crypto:` backend.
- **`load_curve_crypto` is a CLI module method** — shared between
  `omq keygen` and socket runners.

## 0.1.0 — 2026-03-31

Initial release — CLI extracted from the omq gem (v0.8.0).
