# OMQ::QoS -- Delivery Guarantees for OMQ

[![CI](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml/badge.svg)](https://github.com/zeromq/omq.rb/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/omq-qos?color=e9573f)](https://rubygems.org/gems/omq-qos)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%203.3-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

Per-hop delivery guarantees for [OMQ](https://github.com/zeromq/omq.rb),
inspired by MQTT QoS levels. Adds ACK-based at-least-once delivery using
xxHash message identification.

```ruby
require "omq"
require "omq/qos"

push = OMQ::PUSH.new(nil, qos: 1)
push.connect("tcp://worker-1:5555")
push.connect("tcp://worker-2:5555")
push << "reliably delivered"
# If worker-1 dies, unacked messages retry on worker-2.
```

## QoS Levels

| Level | Name            | Behavior                                      |
|-------|-----------------|-----------------------------------------------|
| 0     | Fire-and-forget | Default ZMQ behavior (no overhead)             |
| 1     | At-least-once   | Receiver ACKs; sender retries on connection loss |

## How it works

`require "omq/qos"` prepends onto OMQ routing strategies. No
monkey-patching of core send/receive paths — the prepends activate only
when `qos >= 1`:

- **Sender** (PUSH, SCATTER): tracks sent messages in a pending store
  keyed by xxHash digest. An ACK listener reads ACK command frames from
  each peer. On disconnect, unacked messages are re-enqueued for
  delivery to the next peer.
- **Receiver** (PULL, GATHER): sends an ACK command frame back to the
  sender after each message is received.
- **REQ/REP**: the reply IS the ACK. At QoS 1, if the connection drops
  before a reply arrives, the request is transparently re-sent to the
  next REP.

Fan-out patterns (PUB/SUB, XPUB/XSUB, RADIO/DISH) are deliberately out
of scope — see the RFC for the rationale.

### ACK protocol

ACKs are ZMTP command frames (invisible to applications):

```
Name: "ACK"   Data: 'x' + XXH64(wire_bytes)    # 9 bytes total
```

The hash covers raw ZMTP wire bytes (frame headers + bodies), so
different framings of the same payload produce different digests.

### Backpressure

Pending (un-ACK'd) messages count toward `send_hwm`. When the pending
store is full, `send` blocks in the fiber until an ACK arrives or the
connection drops (which re-enqueues the stuck messages). A misbehaving
peer that never ACKs will stall the sender rather than grow the store
unboundedly.

### Linger and pending messages

`Socket#close` with `linger: 0` discards anything that hasn't yet been
ACK'd. This is correct but worth calling out: with QoS 1, messages you
sent just before closing — even successfully written on the wire — can
still be lost if the ACKs hadn't come back yet. Set `linger` to a
non-zero value (or `Float::INFINITY`) if you need the close to wait
for outstanding ACKs.

### Zero overhead at QoS 0

At QoS 0 (the default), no pending store is created, no ACK commands are
sent, and no xxHash is computed. The prepended methods check
`engine.options.qos` and fall through to the original behavior.

## Supported socket types

| Sender         | Receiver        | ACK mechanism       |
|----------------|-----------------|---------------------|
| PUSH / SCATTER | PULL / GATHER   | ACK command frame   |
| REQ            | REP             | Reply = ACK         |

## Requirements

- Ruby >= 3.3
- [omq](https://github.com/zeromq/omq.rb) >= 0.12
- [xxhash](https://rubygems.org/gems/xxhash) (C extension)

## RFC

See [rfc/zmtp-qos.md](rfc/zmtp-qos.md) for the full specification.
