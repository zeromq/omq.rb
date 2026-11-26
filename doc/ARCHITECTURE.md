# OMQ Architecture

Pure Ruby ZeroMQ. [protocol-zmtp](../gems/protocol-zmtp) speaks ZMTP 3.1;
Async fibers do the I/O.

## Layers

```
Application    OMQ::PUSH, OMQ::SUB, ...
Socket         send / receive / bind / connect
Engine         connection lifecycle, reconnect, linger, task tree
Routing        work-stealing, fan-out, REQ/REP envelope, identity tables
Connection     ZMTP handshake, heartbeat, framing (protocol-zmtp)
Transport      TCP, IPC, inproc, UDP, plugins
IO             io-stream (buffered), Async (Fiber::Scheduler)
```

## Components

| Class | File | Role |
|---|---|---|
| `OMQ::Socket` | `lib/omq/socket.rb` | Public API: bind, connect, send, receive, close, options |
| `OMQ::Engine` | `lib/omq/engine.rb` | Per socket: listeners, dialers, connections, linger, monitor |
| `Engine::SocketLifecycle` | `lib/omq/engine/socket_lifecycle.rb` | Open/closing/closed state, parent task, socket barrier, peer promises |
| `Engine::ConnectionLifecycle` | `lib/omq/engine/connection_lifecycle.rb` | One connection: handshake, ready, teardown |
| `Engine::Reconnect` | `lib/omq/engine/reconnect.rb` | Dial loop with backoff |
| `Engine::RecvPump` | `lib/omq/engine/recv_pump.rb` | Reads one connection into the recv queue |
| `Engine::Heartbeat` | `lib/omq/engine/heartbeat.rb` | PING/PONG and peer TTL for one connection |
| `Routing::*` | `lib/omq/routing/` | One strategy per socket type |
| `Transport::*` | `lib/omq/transport/` | TCP, IPC, inproc, UDP |
| `OMQ::Reactor` | `lib/omq/reactor.rb` | Shared `omq-io` thread for callers outside Async |
| `OMQ::MonitorQueue` | `lib/omq/monitor_queue.rb` | Monitor events; lifecycle events never block the engine |
| `Protocol::ZMTP::Connection` | `gems/protocol-zmtp` | Framing, handshake, NULL/PLAIN/CURVE, PING/PONG |

Routing strategies:

| Behavior | Classes | Shared code |
|---|---|---|
| Fair-queue receive only | `Pull`, `Gather` | |
| Work-stealing send only | `Push`, `Scatter` | `RoundRobin` |
| Work-stealing both ways | `Pair`, `Dealer`, `Client`, `Channel` | `RoundRobin` |
| Request-reply | `Req`, `Rep` | `RoundRobin` |
| Identity routing | `Router`, `Server`, `Peer` | `ConnSendPump` |
| Fan-out | `Pub`, `XPub`, `Radio` | `FanOut`, `PrefixSet` (radix trie) |
| Subscription receive | `Sub`, `XSub`, `Dish` | |

Every strategy includes `ClosableQueues`, so closing the socket closes its
queues.

## Task tree

A new socket spawns nothing. The first `#bind` or `#connect` captures the
parent (the current task, the `Reactor` root, or the `parent:` kwarg: any
object with `#async`) and creates the socket barrier under it.

```
parent task
+-- socket barrier            SocketLifecycle#barrier
    |-- accept loops          one per listener
    |-- mechanism maintenance
    |-- conn tasks            accepted connections: handshake, wait until done
    |-- conn supervisors      one per connection
    |-- dialer barrier        one per connected endpoint
    |   |-- reconnect loop
    |   +-- conn tasks        dialed connections
    +-- connection barrier    ConnectionLifecycle#barrier, one per connection
        |-- send pump         per connection
        |-- recv pump         or reaper on PUSH/SCATTER
        |-- heartbeat         sends PINGs from a child fiber
        +-- subscription listener   PUB, XPUB, RADIO
```

- Stopping the socket barrier cancels every task (`#close`, `#stop`).
  `#disconnect` stops only that endpoint's dialer barrier.
- The first task to finish in a connection barrier means the connection is
  dead. Its supervisor waits on that barrier from outside it, so teardown can
  stop the barrier without cancelling itself.
- The reaper reads from write-only connections, so a dead peer is noticed
  before the next send.
- All tasks are transient. During linger, `#close` blocks in the caller's
  fiber, which keeps the reactor alive.

## Connection lifecycle

States: `new -> handshaking -> ready -> closed`. Inproc skips the handshake.

Peer loss, `#close` and `#stop` all end in `ConnectionLifecycle#tear_down!`:
routing removal, connection close, `:disconnected` event, done promise,
all-peers-gone check, reconnect (dialed connections), then the connection
barrier stops. The state guard makes it run once, however many pumps
report the loss.

A peer that breaks the protocol or exceeds a limit (frame size, parts per
message, subscriptions, handshake properties) is disconnected. Its socket
keeps running.

## Send path

### Send queues and HWM

Work-stealing sockets (PUSH, SCATTER, DEALER, REQ, PAIR, CLIENT, CHANNEL)
have one send queue per socket, bounded by `send_hwm`. Every connection's
send pump races to drain it. A slow peer's pump blocks on its own flush
while the others keep draining. Messages queued before any peer connects
wait in the same queue.

Fan-out (PUB, XPUB, RADIO) and identity routing (ROUTER, SERVER, PEER) have
one bounded queue per connection. Fan-out filters by subscription in the
pump, and drops for a mute subscriber by default (`on_mute`). ROUTER drops
for a full peer unless `router_mandatory`.

A pump blocks for the first message, takes more without blocking up to a
batch cap, writes them all, then flushes once. Fan-out encodes a message
once and writes the same bytes to every unencrypted subscriber.

A message a pump already took is lost if its peer dies before the write,
as in libzmq. ZMTP has no acknowledgements.

A work-stealing socket with a single inproc peer skips the send queue and
the recv pump: the sender enqueues into the peer's recv queue.

## Receive path

Each strategy has one recv queue per socket, bounded by `recv_hwm`. Every
connection's recv pump writes into it. A pump yields after a message and
byte budget, so a busy peer can't starve the others. Order holds within a
connection, not across connections.

When the recv queue is full the pump stops reading. TCP backpressure
reaches the peer, and the heartbeat doesn't expire a connection we stopped
reading.

Received parts are frozen binary strings.

## Close and linger

`#close`:

1. Stops listeners if any peer is connected.
2. Drains send queues for up to `linger` (default forever, like libzmq). With
   no peer yet, listeners stay open so a late peer still gets the queue.
3. Stops listeners and the socket barrier.
4. Closes routing queues: blocked and later `#receive` calls return nil,
   `#send` raises `OMQ::SocketClosedError`.
5. Emits `:closed` and closes the monitor queue.

`#stop` does the same without the drain.

## Reconnect

`Engine#connect` builds the transport's dialer and starts a reconnect loop
in the endpoint's dialer barrier. Retries wait `reconnect_interval`; a Range
backs off exponentially up to its end (endless: no cap). A lost dialed
connection schedules the loop again unless the socket is closing. Inproc
connect before bind uses the same loop.

## Heartbeat

With `heartbeat_interval`, a connection sends PINGs and expires when nothing
arrives within `heartbeat_timeout` of the oldest unanswered PING. PINGs
advertise TTL 0 unless `heartbeat_ttl` is set. A peer's TTL is honored even
without a local interval. ZMTP 3.0 peers get no PINGs.

## Cancellation safety

`Async::Cancel` can reach a pump at any blocking call. A plain ZMTP frame is
two writes (header, body), so a cancel between them would corrupt the
stream. `Protocol::ZMTP::Connection` wraps every write in `defer_cancel`:
cancellation waits for the frame boundary. Errors from the write itself
(`EPIPE`, `ECONNRESET`) still raise at once.

## Transports

`Engine.transports` maps schemes to transport modules. Built in: `tcp`,
`ipc`, `ruby` (`inproc` falls back to `ruby` unless a plugin registers it),
and `udp` with `omq/radio_dish`. Plugins register more (`lz4+tcp`,
`zstd+tcp`, `ws`, `wss`).

A transport implements:

- `.listener(endpoint, engine)`: a listener with
  `#start_accept_loops(parent, &on_accepted)`, `#stop`, `#endpoint`, and
  optionally `#port`.
- `.dialer(endpoint, engine)`: a dialer with `#endpoint` and `#connect`.
  `Engine#connect` and `Reconnect` use it for every transport.
- Optionally `.validate_endpoint!(endpoint)` and
  `.canonical_endpoint(endpoint)`.

`CONNECTION_LOST` and `CONNECTION_FAILED` accept plugin error classes until
the first bind or connect freezes them.

- **TCP**: IPv4 and IPv6; port `0` or `*` picks one.
- **IPC**: Unix sockets, Linux abstract namespace (`ipc://@name`). Bind
  replaces only a stale socket file; unbind removes the file it created.
- **inproc**: two linked engines pass frozen part arrays through queues. No
  ZMTP framing, no kernel.
- **UDP**: RADIO/DISH datagrams, IPv6 and multicast groups.

TCP and IPC connections are wrapped in `IO::Stream::Buffered`.

## Backends

`backend: :libzmq` (`gems/omq-backend-libzmq`) runs libzmq through FFI on a
dedicated thread that owns the `zmq_socket`. `backend: :rust`
(`gems/omq-backend-rust`) wraps omq.rs. Both implement the `Engine`
contract behind the same `Socket` API.

## Differences from libzmq

- **Per-socket send HWM.** libzmq bounds each pipe, so `send_hwm` 1000 with
  10 peers buffers 10 000 messages, and strict round-robin queues message
  N+1 behind a slow worker. OMQ's work-stealing queue means `send_hwm`
  messages total, and a stuck peer only parks its own pump. Under tight
  bursts one pump can take a whole batch, so distribution isn't strict
  round-robin.
- **No readiness FD.** libzmq's `ZMQ_FD` is edge-triggered and can miss
  wakes. OMQ's `#receive` blocks on a queue of fully decoded messages.
- **REQ after peer loss.** REQ accepts a new request once the peer holding
  the unanswered one disconnects. libzmq needs `ZMQ_REQ_RELAXED` for that.
