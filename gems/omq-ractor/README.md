# OMQ::Ractor -- Networked Ractors

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-ractor?color=e9573f)](https://rubygems.org/gems/omq-ractor)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Ruby Ractors give you true parallelism -- each Ractor gets its own GVL,
so CPU-bound work runs on separate cores. But they can only talk to each
other inside a single process, using `Ractor::Port`. No networking, no
message patterns, no load balancing.

`OMQ::Ractor` changes that. It connects Ractors to OMQ sockets -- a
pure Ruby messaging library with TCP, IPC, and in-process transports.
Your Ractors can now talk across processes, across machines, using
patterns like load-balanced pipelines, publish/subscribe, and
request/reply. All in pure Ruby, no C extensions.

The I/O stays in the main Ractor (on the Async fiber scheduler). Worker
Ractors do pure computation. Messages flow between them transparently,
serialized per-connection: zero-copy for in-process, Marshal for the
network.


## The problem

Ractors and Async don't mix. `Async::Queue` wraps a `Thread::Queue`
internally -- it can't be shared between Ractors or even copied into
one. So you can't just pass an Async queue to a Ractor and have objects
flow between them.

`Ractor::Port#receive` blocks the fiber scheduler. Calling it inside
Async freezes the entire reactor -- no other fibers run until the port
has data. Same for `Ractor#join` and `Ractor#value`.

Without OMQ::Ractor, connecting Ractors to the network means writing
your own bridge: threads, pipes, queues, serialization, error handling.
For every direction, every transport.


## Usage

```ruby
require "omq"

Async do
  pull = OMQ::PULL.bind("tcp://0.0.0.0:5555")
  push = OMQ::PUSH.connect("tcp://results.internal:5556")

  worker = OMQ::Ractor.new(pull, push) do |omq|
    pull_p, push_p = omq.sockets  # handshake (must be first call)

    while msg = pull_p.receive    # nil on close
      push_p << expensive_transform(msg)
    end
  end

  worker.join
end
```

The block runs inside a Ruby Ractor with its own GVL. `omq.sockets`
performs a setup handshake and returns `SocketProxy` objects --
lightweight wrappers around `Ractor::Port` pairs.

### Multiplexing with Ractor.select

`Ractor.select` waits on multiple `Ractor::Port` objects and returns
`[port, value]`. Use `#to_port` to get the underlying port, and
`#socket_for` to map back to the proxy:

```ruby
worker = OMQ::Ractor.new(pull_a, pull_b, push) do |omq|
  sockets = omq.sockets
  a, b, out = sockets

  loop do
    port, msg = Ractor.select(a.to_port, b.to_port)
    break if msg.nil?  # socket closed
    source = sockets.socket_for(port)  # => a or b
    out << process(source, msg)
  end
end
```

Note: `Ractor.select` returns raw port values, bypassing `SocketProxy#receive`.
For topic-based sockets, `msg` will be the full `[topic, payload]` array --
use `#receive` or `#receive_with_topic` on a single proxy instead if you
need topic stripping.

### Bidirectional (PAIR, REQ/REP, DEALER)

```ruby
worker = OMQ::Ractor.new(pair) do |omq|
  p = omq.sockets.first

  while msg = p.receive
    p << transform(msg)
  end
end
```

### PUB/SUB with topics

```ruby
worker = OMQ::Ractor.new(pub) do |omq|
  pub_p = omq.sockets.first

  pub_p << obj                             # all subscribers (empty topic)
  pub_p.publish(obj, topic: "prices.")     # matching subscribers only
end

worker = OMQ::Ractor.new(sub) do |omq|
  sub_p = omq.sockets.first

  obj = sub_p.receive                      # payload only (topic stripped)
  topic, obj = sub_p.receive_with_topic    # both
end
```

Topic prefix matching works normally. The topic stays as a plain string
frame; only the payload is serialized.

### Worker pool

PUSH round-robins across connected peers. Multiple Ractors on the same
endpoint = parallel workers:

```ruby
Async do
  source = OMQ::PUSH.bind("inproc://work")
  sink   = OMQ::PULL.bind("inproc://results")

  workers = 4.times.map do
    pull = OMQ::PULL.connect("inproc://work")
    push = OMQ::PUSH.connect("inproc://results")

    OMQ::Ractor.new(pull, push) do |omq|
      p_in, p_out = omq.sockets
      while msg = p_in.receive
        p_out << expensive_transform(msg)
      end
    end
  end

  # Feed work, collect results
  100.times { |i| source << job(i) }
  100.times { sink.receive }
end
```


## Per-connection serialization

With `serialize: true` (default), messages are automatically converted
between Ruby objects and wire-format bytes:

    transport   send                        receive
    ---------   --------------------------  ---------------
    inproc      Ractor.make_shareable       pass-through
                (freeze in place, no copy)
    ipc/tcp     Marshal.dump                Marshal.load
                (cached for fan-out)

Serialization happens at the connection level, not the socket level. A
single socket with both inproc and tcp connections serializes differently
for each.

For ipc/tcp, a SerializeCache ensures fan-out (PUB to N subscribers)
calls Marshal.dump once per message regardless of subscriber count.

Use `serialize: false` for raw messages (frozen string arrays):

```ruby
worker = OMQ::Ractor.new(pull, push, serialize: false) do |omq|
  p_in, p_out = omq.sockets
  while msg = p_in.receive          # frozen string array, e.g. ["hello"]
    p_out << [msg.first.upcase]     # must send frozen string arrays
  end
end
```

With `serialize: true`, both ends of a tcp/ipc connection must agree on
the format. Two OMQ::Ractor instances communicate Ruby objects
transparently. Mixing Ractor-wrapped and regular sockets over tcp/ipc
requires `serialize: false`.


## Architecture

```
Main Ractor (Async)                 Worker Ractor
-------------------                 --------------
socket.receive ---> input_port ---> proxy.receive
  (Async fiber)     (worker owns)     (user code)

socket.send    <--- output_port <--- proxy.<<
  (Async fiber)     (main owns)       (user code)
      ^
      |
  IO.pipe + Thread::Queue
  (Thread does port.receive,
   signals Async via pipe)
```

Input bridge: Async fiber reads from socket, sends to worker's input
port. Ractor::Port#send is non-blocking, safe in Async.

Output bridge: a Thread reads from the worker's output port
(port.receive blocks the fiber scheduler, can't be an Async fiber),
pushes to a Thread::Queue, and signals an Async fiber via IO.pipe. The
Async fiber drains the queue and feeds the engine directly -- avoiding
a Reactor.run round-trip per message.

Setup handshake: the worker must call `omq.sockets` as its first
action. This creates worker-owned input ports, sends them to the main
Ractor, and returns SocketProxy objects. The main Ractor waits up to
100ms; if the handshake doesn't complete, the Ractor is stopped and
an error is raised.


## Performance

Inproc, Ruby 4.0.2 +YJIT, 4-core VM. Speedup relative to inline
(single-core, no Ractor):

```
                 bare Ractor    OMQ::Ractor
                 -----------    -----------
fib(30) ~25ms/call, 200 items:
  1 worker:        1.0x           0.9x
  2 workers:       1.7x           1.6x
  4 workers:       3.1x           2.3x

fib(32) ~61ms/call, 100 items:
  1 worker:        1.0x           0.8x
  2 workers:       1.6x           1.4x
  4 workers:       2.5x           2.2x
```

Bare Ractors top out around 2.5-3.1x on 4 cores. fib allocates no
objects (small Integers are immediate values), so this isn't GC -- it's
Ruby's Ractor overhead itself (YJIT code cache contention, VM internal
locks, OS thread scheduling). OMQ adds a 5th thread (main reactor)
competing for 4 cores. The gap narrows with heavier work per message
(0.8x at 25ms, 0.3x at 61ms).

Bridge overhead (passthrough, no CPU work):

```
Baseline (no Ractor):  528k msg/s   1.9 us/msg
OMQ::Ractor:           149k msg/s   6.7 us/msg
```

Reactor responsiveness during CPU work:

```
Echo latency while 50x fib(30) crunches in Ractor:
  p50: 54 us    p95: 3.1 ms (GC)    max: 13 ms (GC)

Without Ractor: reactor blocked for 1252ms
```


## Limitations

- Worker Ractors do pure computation. No Async, no I/O scheduling, no
  fiber scheduler. All I/O stays in the main Ractor.

- Each OMQ::Ractor wraps its own socket instances. For parallel workers,
  create multiple Ractors with separate sockets connected to the same
  endpoint (see worker pool above).

- `omq.sockets` must be the first call in the block. Doing anything else
  before the handshake triggers a timeout error.

- With `serialize: true` over tcp/ipc, both ends must use OMQ::Ractor
  (or handle Marshal encoding manually). Use `serialize: false` when
  talking to regular sockets or non-Ruby peers.
