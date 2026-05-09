# How OMQ Ruby got fast

A technical article on the design choices and dead ends behind the
throughput numbers in [`../bench/README.md`](../bench/README.md).
Audience: maintainers of other ZMQ implementations and performance-
curious Rubyists who want to understand which optimisations stack and
which look promising on paper but lose throughput in practice.

This is not a tour of the codebase -- the architecture docs cover that.
It is the story of how the wire throughput got to where it is, told
in the order the decisions landed.


## Premise

Pure Ruby has no business being fast at message passing over TCP.
The ZMTP 3.1 wire protocol is byte-level framing with variable-
length headers, greeting negotiation, mechanism handshake, and
per-frame encoding -- the kind of work that C libraries do in tight
loops with zero-copy semantics and dedicated I/O threads.

libzmq separates the application from a dedicated I/O thread. The
app encodes and hands a message to the I/O thread via a lock-free
pipe; the I/O thread does the actual `send()`. At small message
sizes (128-512 B) that overlap is the primary advantage: while the
app encodes message N+1, the I/O thread writes message N. A naive
single-threaded implementation cannot keep up.

OMQ takes a different approach: everything runs on Async fibers
(cooperative scheduling via `Fiber::Scheduler`), backed by epoll on
Linux (or io_uring when `liburing-dev` is installed). No threads
within sockets, no lock-free pipes. The question was whether
cooperative fibers plus careful batching could compensate for the
lack of true I/O-thread concurrency.

The answer turned out to be yes, with a fair amount of work along
the way.


## Starting point: two queues per socket

The core architecture is deliberately simple. Each socket has
exactly one inbound queue and one outbound queue -- not one per
peer. Per-connection pump fibers push decoded messages into the
one inbound queue and pull messages from the one outbound queue.

The outbound queue's bound is the socket's HWM. Backpressure is a
single cap, not a per-peer matrix. Slow peers do not corner the
socket: a blocked pump leaves messages in the shared outbound
queue; faster pumps steal them. Head-of-line blocking patterns
where one non-draining peer freezes the socket do not arise.

This contrasts with libzmq's per-pipe-per-peer pattern, which
mirrors ZMTP wire framing into the socket's internal data
structures. libzmq needs that complexity for its dedicated-I/O-
thread design; without it the I/O thread has no way to multiplex
peers fairly. The two-queue design lifts multiplexing into work-
stealing on the outbound queue, which makes the implementation
substantially smaller.


## Adopting io-stream for buffered I/O

The first version used hand-rolled socket I/O: raw `read` and
`write` calls on the TCP socket, one syscall per frame header read,
one per payload read.

Replacing that with `IO::Stream::Buffered` from the `io-stream` gem
brought read-ahead buffering (fewer syscalls for frame parsing),
automatic `TCP_NODELAY`, and exact-byte reads via `#read_exactly`.

TCP throughput improved 20-28 % from read-ahead buffering alone.
The frame parser went from multiple syscalls per message to
amortised reads out of a userspace buffer. On the write side,
`io-stream` auto-flushes at 64 KiB, which turned out to be a
critical property for later batching work (see below).


## Batched send pump flushes

Before this change, every `Connection#send_message` call did a
write *and* a flush -- one syscall per message. Under burst load
with N messages queued and M connections, that was N × M flushes
per cycle.

The fix split `#send_message` into `#write_message` (buffer only)
and `#flush` (syscall). Send pumps now drain all queued messages
before flushing, reducing flush count from N × M to just M per
batch cycle.

| Pattern | Transport | Before | After |
|---|---|---|---|
| PUSH/PULL | IPC/TCP | baseline | 3.0-3.4× |
| PUB/SUB (10 subs) | IPC/TCP | baseline | 3.4-4.0× |

This was the single largest throughput improvement in the project's
history. The lesson: when the runtime auto-flushes at a buffer
threshold (64 KiB for `io-stream`), explicit per-message flushes
are pure overhead. Drain the queue, then flush once.


## Inproc send queue bypass (Pipe)

Inproc connections do not cross the kernel, so the ZMTP codec and
`io-stream` buffering are unnecessary overhead. But the original
inproc transport still routed messages through the shared send
queue and a send pump fiber -- two queue hops per message.

The bypass: when a round-robin socket (PUSH, SCATTER, REQ, DEALER,
CLIENT) has exactly one inproc peer, `#enqueue` writes directly
into the peer's recv queue, skipping both the send queue and the
send pump fiber.

PUSH/PULL inproc went from ~200k to ~1.35M msg/s (6.7×). PAIR
inproc went from ~200k to ~1.38M msg/s (6.8×), with latency
dropping from 4.9 µs to 725 ns.

The bypass falls back to the normal send queue automatically when
a second peer connects or the inproc peer disconnects.


## Uncapping the send queue drain

The send pump originally capped each drain pass at 64 messages.
Combined with `io-stream`'s 64 KiB auto-flush, writes hit the wire
naturally under sustained load -- but the explicit flush after each
64-message batch added unnecessary syscalls.

Removing the cap and draining the entire queue in one pass, with a
single explicit flush at the end, cut IPC latency by 12 % and TCP
latency by 10 %.


## Eliminating transform dispatch in the recv pump

The recv pump originally took a `transform:` lambda parameter that
was called on every received message. For socket types with no
transform (PUSH/PULL, PUB/SUB -- the majority), this was a no-op
lambda invocation on the hot path.

Replacing the lambda with block captures and splitting into two
entry methods -- one for the no-transform path, one for the
transform path (REQ, REP, ROUTER, SERVER, PEER) -- made the common
path branch-free and gave YJIT a monomorphic call site to
specialise.

Benchmark results after this change with YJIT enabled:

| Transport | Throughput | Latency |
|---|---|---|
| inproc | 229k msg/s | 11 µs |
| IPC | 49k msg/s | 52 µs |
| TCP | 36k msg/s | 63 µs |

YJIT speedup was approximately 2.5× on inproc and 2× on IPC/TCP.
These numbers are from an early version; later optimisations pushed
all of them substantially higher.


## Recv pump fairness limits

Without fairness limits, a fast or large-message producer that kept
the `io-stream` buffer full could spin the recv pump indefinitely
without yielding, blocking other connections' pumps from running.

The fix: each recv pump yields to the fiber scheduler after reading
64 messages or 1 MiB from one connection, whichever comes first.
The cap was later bumped to 256 messages / 512 KiB, symmetric with
the send-side batch caps.

This was not a throughput optimisation per se -- single-peer
throughput is unaffected. But without it, multi-peer fairness was
broken: a fast producer could starve its siblings entirely.


## Reducing allocations on the send hot path

Several allocation-reduction passes targeted the send path:

**`#freeze_message` short-circuiting.** The message freezing step
(which ensures all parts are frozen binary strings for safe inproc
sharing) checked every part on every send. Adding a fast path that
short-circuits when all parts are already frozen and binary-tagged
eliminated the redundant work for the common case of pre-frozen
string literals. Up to +55 % on small messages.

**Reusing batch arrays.** Send pump strategies allocated fresh
arrays, Sets, and Hashes on every batch cycle. Switching to
persistent instance variables (`@batch`, `@written`, `@latest`)
that get cleared between cycles removed per-batch allocation
pressure.

**Pre-freezing constants.** The empty delimiter frames used by
REQ/REP were allocated fresh on each envelope. Pre-freezing them
as constants eliminated a per-message allocation on the
request-reply path.

**Removing redundant `.b` calls.** Subscription matching in fan-out
called `.b` (force binary encoding) on both sides of the
comparison, but both were already binary. Removing the redundant
calls eliminated two string allocations per subscription check.


## Per-peer HWM, then back to per-socket

An intermediate version switched to per-connection send and recv
queues to match the ZeroMQ spec ("HWM=1000 means 1000 slots per
peer, not total"). This added per-peer send pump fibers, a staging
queue for messages buffered before any peer connects, and a
per-connection FairQueue for recv-side round-robin.

The per-peer model turned out to be over-engineered:

- Per-connection inner queues and the round-robin drain cycle
  preserved an across-connection FIFO guarantee that PUSH
  semantics do not actually promise.
- Per-connection HWM gave rate isolation but no useful
  backpressure -- a slow consumer just filled the parent queue
  faster.
- The staging queue added a prepend-on-disconnect path that
  pretended to preserve ordering on reconnect, which is a
  guarantee ZMQ does not make.

The revert to per-socket HWM with work-stealing send pumps removed
the staging queue, the per-connection queue map, and the double-
drain race condition. One shared bounded queue per socket, drained
by N per-connection send pumps that race to dequeue. Per-pump batch
caps enforce fairness across the work-stealing pumps.

This is strictly better PUSH semantics than libzmq's strict
per-pipe round-robin, which is a known footgun where one slow
worker stalls the whole pipeline.


## Calibrating the batch cap

The initial batch cap of 64 messages was too aggressive for large
messages: with 64 KiB messages it forced a flush after every ~4 MB,
capping throughput at roughly 50 % of what the network could
handle.

Switching to a dual cap (256 messages OR 512 KiB, whichever comes
first) let large-message workloads batch into ~8 messages of 64 KiB
per cycle while small-message workloads still hit the message cap
quickly enough that other per-connection pumps get a fair turn.

Selected bench deltas:

| Pattern | Transport | Size | Improvement |
|---|---|---|---|
| PUSH/PULL | TCP 1p | 8 KiB | +33.5 % |
| PUSH/PULL | IPC 3p | 64 B | +27.5 % |
| PUSH/PULL | IPC 1p | 8 KiB | +22.3 % |
| PUSH/PULL | TCP 1p | 64 KiB | +20.3 % |
| ROUTER/DEALER | IPC 3p | 64 B | +14.6 % |

28 improvements, 0 regressions, 42 stable across the full matrix.


## Batched `#write_messages` under a single mutex

Each `Connection#send_message` acquires and releases the
connection's mutex. When the send pump drains a batch of N messages,
that is N mutex round-trips per connection.

`Protocol::ZMTP::Connection#write_messages` collapses the batch
into one mutex acquire/release pair. The `size == 1` path still
uses `#send_message` (write + flush in one lock) to avoid a
second mutex round-trip for a separate `#flush` call.

PUSH/PULL inproc: +18-28 %. TCP/IPC: flat to +17 %.


## PUB/RADIO fan-out: pre-encode once, write to all

The fan-out path for PUB and RADIO originally encoded ZMTP frames
per subscriber. For N subscribers receiving the same message, that
meant N redundant `Frame.new` + `#to_wire` calls.

`Frame.encode_message` now encodes the wire bytes once. All non-
CURVE connections receive the pre-encoded bytes via
`Connection#write_wire`, bypassing per-connection encoding
entirely.

CURVE connections still encode per-connection because each has its
own nonce sequence.


## YJIT-friendly hot path patterns

Several micro-optimisations targeted YJIT's specialisation
behaviour:

**Monomorphic recv pump methods.** The recv pump's no-transform and
transform entry points are separate methods, each with a stable
call-site signature. YJIT specialises each independently rather
than generating a polymorphic dispatch.

**`Array#first` over `Array#[0]`.** `#first` has a dedicated YJIT
specialisation that beats `#[0]` on single-element arrays -- the
common case for single-frame messages.

**Size-1 fast paths.** Byte-counting in the recv pump and batch
accounting in the send pump short-circuit for single-frame messages
(the common case), skipping the `while` loop and the
`sum`-with-block allocation.

**Removing safe navigation.** `&.` on the hot path forces YJIT to
emit a nil check branch on every call. Replacing `obj&.method` with
a direct call where nil is structurally impossible gave YJIT a
cleaner graph to optimise.

**O(1) subscribe-all fast path.** `FanOut#subscribed?` gained a
`@subscribe_all` Set for connections subscribed with an empty
prefix, turning the match-all check from a linear scan into a set
membership test.


## Deleting FairQueue: one bounded queue per socket

The FairQueue abstraction aggregated per-connection bounded recv
queues with round-robin delivery. It was mechanically correct but
unnecessary: recv-pump fairness limits (see above) already
prevented any single connection from starving others, and the
per-connection inner queues added allocation overhead without
providing useful backpressure.

Replacing FairQueue with a single `Async::LimitedQueue` sized to
`recv_hwm` meant recv pumps write directly into one shared queue.
The FairQueue class, SignalingQueue, and the FairRecv mixin were
deleted entirely.


## Reducing per-message allocations in send pumps (v0.22.1)

A focused allocation-reduction pass consolidated several patterns:

- **`Routing.dequeue_batch`**: merged blocking-dequeue + non-
  blocking sweep into one method, reusing the batch array across
  pump cycles.
- **REP envelope**: switched from `Hash + splat` to
  `[conn, envelope] + concat`, eliminating a hash allocation per
  reply.
- **REQ `#transform_send`**: `dup.unshift` instead of splat,
  avoiding an intermediate array.


## Frozen + BINARY message contract

Messages crossing transport boundaries need a consistent encoding
contract. The original approach called `.b` (force binary encoding)
on every part on every send -- a per-part string allocation even
when the part was already binary.

The current contract:

- `Writable#send` coerces non-String parts via `#to_str`, re-tags
  unfrozen parts to `Encoding::BINARY` in place (a flag flip, not
  an allocation), and freezes every part plus the array.
- Receivers always see frozen binary-tagged parts: TCP/IPC via
  `byteslice` plus recv-pump freeze, inproc via `Pipe#send_message`
  which only allocates a fresh binary copy for the pathological
  case of a frozen non-BINARY part.

Cost: approximately 20-30 % inproc throughput (the freeze overhead
on the hot path). TCP/IPC are unaffected because the wire encoding
already produces binary strings. The trade-off is worth it: mutation
bugs surface as `FrozenError` instead of silently corrupting a
shared reference on inproc.


## Transport-supplied Connection class (v0.27.0)

Transport modules may now define `.connection_class` to substitute
their own `Protocol::ZMTP::Connection`-shaped class. This is not
a throughput optimisation directly, but it enables plugin transports
whose wire shape differs from ZMTP/3.1 (e.g. WebSocket per RFC 45)
to plug in without forking the engine -- which in turn keeps the
hot path clean of conditional transport checks.


## Where the numbers stand

Current throughput on a Linux VM (2018 Mac Mini), Ruby 4.0.2 +YJIT,
single peer:

| Pattern | Transport | 128 B msg/s | 32 KiB msg/s |
|---|---|---|---|
| PUSH/PULL | inproc | ~1.75M | ~1.85M |
| PUSH/PULL | IPC | ~500k | ~60k |
| PUSH/PULL | TCP | ~500k | ~57k |
| REQ/REP (RTT) | inproc | 6.6 µs | 6.8 µs |
| REQ/REP (RTT) | TCP | 50 µs | 73 µs |
| PAIR | inproc | ~1.65M | ~1.81M |
| PAIR | TCP | ~510k | ~55k |
| ROUTER/DEALER | TCP 3p | ~468k | ~51k |

Inproc throughput is effectively flat across message sizes because
no bytes cross the kernel -- the Pipe bypass passes the frozen
String by reference. TCP/IPC throughput scales inversely with
message size as kernel buffer copies dominate.


## Things tried and dropped

### Per-peer HWM (reverted)

Described above in detail. The per-connection send/recv queue model
matched the ZeroMQ spec but added complexity (staging queue,
per-connection pump fibers, double-drain race) without improving
throughput or semantics. Reverted in favour of per-socket HWM with
work-stealing.

### Receive prefetch with Mutex

An early optimisation added prefetch buffering to `#receive`:
drain up to 64 messages from the queue behind a Mutex on each call,
then return from the local buffer on subsequent calls. This gave
a dramatic improvement at the time (TCP 64 B: 30k → 221k msg/s)
when the recv path had more overhead. Later changes (FairQueue
deletion, direct LimitedQueue access) made the prefetch buffer
redundant -- the queue is already bounded and fast -- and it was
absorbed into the simpler direct-dequeue path.

### Per-connection recv queues (deleted with FairQueue)

FairQueue aggregated per-connection bounded recv queues with
round-robin delivery. The per-connection queues were meant to
provide per-peer rate isolation, but in practice a slow consumer
just filled whichever queue the recv pump wrote to next. The
fairness guarantee that mattered -- preventing one fast producer
from starving others -- was better handled by the recv pump's
yield-after-N-messages limit. Deleted along with FairQueue,
SignalingQueue, and the FairRecv mixin.

### Recv-side batching

The recv pump pushes one message at a time into the socket's
LimitedQueue. A batch-push that enqueues multiple messages per
queue interaction looked like it would reduce per-message queue
overhead. In practice the queue push is already minimal -- the
bottleneck would have to be the LimitedQueue itself for batching
to help, and at that point the design is already at its ceiling.

### Profile-guided YJIT tuning

`--yjit-stats` shows 770k `send_iseq_missing_optional_kw`
fallbacks per benchmark run, 99.8 % of all send fallbacks.
These come from `Thread::SizedQueue#push` and `#pop`, whose C
signatures combine an optional positional (`non_block`) with an
optional keyword (`timeout:`). YJIT cannot fully inline calls
that omit either.

However, `avg_len_in_yjit` is 99.9 % -- the hot path already
runs almost entirely in JIT-compiled code. The "fallback" means
YJIT emits a slightly less optimised (but still native) call
stub, not an interpreter drop. Passing all arguments explicitly
eliminates the counter but adds per-call overhead that either
regresses throughput or shows no consistent improvement. This
is a Ruby/YJIT limitation in `Thread::SizedQueue`, not something
fixable from OMQ.


## Combined header+body write for short frames

Each ZMTP frame on the unencrypted path issued two `@io.write`
calls: one for the 2-byte header, one for the body.  `io-stream`'s
`Writable#write` acquires a `Thread::Mutex` on every call --
approximately 72 ns uncontended on Ruby 4.0.3 + YJIT.  For small
messages the mutex overhead outweighed the actual I/O work.

The fix: short frames (body <= 255 bytes, the ZMTP short-frame
boundary) now build header + body into a reusable `@frame_buf`
and issue a single `@io.write`.  Long frames keep the existing
two-write path to avoid copying large payloads into an
intermediary buffer.

Micro-benchmarks confirmed the crossover at roughly 2 KiB:

| Body size | Two writes | Combined (reusable) | Δ |
|---|---|---|---|
| 8 B | 342 ns | 163 ns | 2.1× |
| 32 B | 344 ns | 166 ns | 2.1× |
| 128 B | 343 ns | 171 ns | 2.0× |
| 512 B | 347 ns | 183 ns | 1.9× |
| 2 KiB | 372 ns | 370 ns | 1.0× |
| 8 KiB | 514 ns | 737 ns | 0.7× |

End-to-end PUSH/PULL TCP, single peer:

| Size | Before | After | Δ |
|---|---|---|---|
| 8 B | 524k msg/s | 563k msg/s | +7.5 % |
| 32 B | 493k msg/s | 516k msg/s | +4.8 % |
| 128 B | 477k msg/s | 512k msg/s | +7.3 % |
| 512 B | 392k msg/s | 416k msg/s | +6.3 % |
| 2 KiB+ | unchanged | unchanged | — |

The gap between 2× micro and 7 % end-to-end prompted a full
pipeline profiling session.  Component breakdown for 8 B messages
over TCP (1795 ns/msg total):

| Component | Cost | Share |
|---|---|---|
| `Writable#send` prep | 125 ns | 7 % |
| `io.write` (write path) | 186 ns | 10 % |
| `Mutex#synchronize` (ZMTP) | 73 ns | 4 % |
| `defer_cancel` | 37 ns | 2 % |
| Queue enq+deq | 306 ns | 17 % |
| `task.yield` / batch | 2 ns | 0.1 % |
| **Receive path** | **~1066 ns** | **59 %** |

`task.yield` costs 559 ns per call but fires once per 256-message
batch -- effectively free per message.  Raising the batch cap would
not help.

The receive path dominates because `Frame.read_from` calls
`io.peek` and `io.read_exactly` per frame, each acquiring
`io-stream`'s internal read mutex.  This is the same per-call
mutex pattern that the write-side fix addressed, but on the read
side it accounts for 59 % of total pipeline cost.  Fixing it
requires changes to `io-stream`'s read path or a batch-read codec
that parses multiple frames from a single buffer -- neither is
feasible without upstream cooperation.


## Where the ceiling is

Every candidate optimisation from the original roadmap has been
investigated.  Recv-side batching, YJIT profiling, and io_uring
(already active via `liburing-dev`) either showed no measurable
gain or hit upstream limitations.  The remaining bottleneck is
`io-stream`'s per-call mutex overhead on the read path, which
accounts for roughly 60 % of the small-message TCP pipeline.
Further gains require either upstream changes to `io-stream` or
a custom batch-read codec that bypasses per-frame mutex
acquisition.
