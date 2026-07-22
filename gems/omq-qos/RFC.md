# ZMTP Quality of Service (QoS)

| Field       | Value                                          |
|-------------|------------------------------------------------|
| Status      | Experimental                                   |
| Editor      | Patrik Wenger <paddor@gmail.com>               |
| References  | [23/ZMTP](https://rfc.zeromq.org/spec/23/), [37/ZMTP](https://rfc.zeromq.org/spec/37/) |

**This specification is experimental.** The design is under active review
and may change in incompatible ways. Implementors should expect revisions.

This specification defines per-hop delivery guarantees for ZMTP 3.1
connections, using ACK/NACK command frames and hash digest-based message
identification.

## License

Copyright (c) 2026 Patrik Wenger.

This Specification is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3 of the License, or (at your option) any
later version.

## Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in [RFC 2119](https://www.ietf.org/rfc/rfc2119.txt).

## Goals

ZeroMQ provides fast, asynchronous messaging but offers no delivery guarantees
beyond TCP's reliable byte stream. Messages can be silently lost during network
partitions, peer crashes, HWM overflow, or reconnection gaps. Applications that
need reliability must build their own acknowledgment layer on top.

This specification adds **per-hop** delivery guarantees inspired by
[MQTT's QoS levels](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234).
The guarantees apply between directly connected peers, not end-to-end across
intermediaries (brokers, proxies).

### Design principles

* **Zero overhead at QoS 0.** The default behavior is unchanged. No ACK frames,
  no tracking, no hash computation.
* **Per-hop, not end-to-end.** Each connection independently enforces its QoS
  level. Multi-hop guarantees require each hop to use QoS.
* **Hash-based identification.** Messages are identified by their XXH64 digest
  rather than sequence numbers. This avoids per-connection state for ID
  allocation.
* **Command-frame ACK/NACK.** Acknowledgments are ZMTP command frames, invisible
  to applications. They flow in the opposite direction to data messages.

### Comparison to MQTT QoS 2

**Dedup** (short for *deduplication*) is the receiver-side check that prevents
the same logical message from being delivered to the application twice. At
QoS >= 2, sender retransmits after connection loss can put the same message on
the wire multiple times; the receiver must recognise the retransmit and drop
it before it reaches the application. This requires the receiver to keep a
small record — a *dedup set* — of the identifiers of messages it has already
delivered.

The protocol choice for QoS 2 therefore turns on two questions: what
**identifier** is used as the dedup key, and when can the receiver **evict**
an entry from the dedup set.

MQTT's QoS 2 uses a **four-way handshake**: `PUBLISH` → `PUBREC` → `PUBREL` →
`PUBCOMP`. The dedup key is a 16-bit **packet ID** chosen by the sender per
session. Packet IDs are *scarce* — the 16-bit namespace gives a session at
most 65 536 distinct outstanding IDs, and because the sender must pick IDs
that are not already in use, each ID has to be **freed** (recycled) after the
exchange completes. `PUBREL` is the sender's signal that the receiver may
forget the packet ID; `PUBCOMP` confirms the release. Without the fourth leg
the sender would not know when it is safe to reuse the ID, and the receiver
would not know when it may evict its dedup entry.

This specification uses a **three-way exchange** (`MESSAGE` → `ACK` → `CLR`)
because the dedup key is the **content hash** of the message (XXH64 over the
raw ZMTP wire bytes). Hashes are derived, not allocated, so there is no
scarcity: a sender never "runs out of" hashes and never needs to reclaim one.
Consequently the receiver does not need the sender's permission to evict a
dedup entry — it can evict on local signals alone:

* **TTL expiry.** Each dedup entry is stamped with the time it was added to
  the set. A periodic sweep (running at half the configured `dedup_ttl`)
  removes any entry whose age exceeds `dedup_ttl`. The TTL SHOULD be chosen
  larger than the sender's `dead_letter_timeout`, so that by the time a
  dedup entry is aged out the sender is guaranteed to have given up on
  retransmitting it.
* **HWM eviction.** The dedup set is bounded at `recv_hwm` entries;
  inserting past the bound evicts the oldest entry first (FIFO).

`CLR` is the fast-path complement to both local signals: the sender emits it
after its own `ACK` processing so the receiver may drop the dedup entry
*promptly* rather than waiting for the TTL sweep. It is not
correctness-critical — a lost `CLR` only costs the memory of one dedup-set
entry until the sweep runs.

Dedup sets are **scoped per connection**: each peer has its own set, keyed by
the post-handshake `PeerInfo` (CURVE public key or `ZMQ_IDENTITY`). Two
different senders transmitting the same bytes to the same receiver do not
collide — they hit separate dedup sets. Collision is only possible when the
*same sender* transmits two distinct-but-byte-identical messages within the
in-flight window.

| Property               | MQTT QoS 2                     | ZMTP QoS 2 (this spec)           |
|------------------------|--------------------------------|----------------------------------|
| Dedup key              | 16-bit packet ID (per session) | 64-bit content hash (XXH64)      |
| Dedup scope            | per session                    | per connection (peer-scoped)     |
| Key scarcity           | High — namespace is 65 536, IDs must be recycled | None — hashes aren't allocated |
| Explicit-release frame | `PUBREL` (required for correctness) | `CLR` (optional fast-path hint) |
| Receiver frees entry   | on `PUBREL`                    | on `CLR`, TTL expiry, or HWM eviction |
| Handshake legs         | 4                              | 3                                |

The tradeoff of hash-based dedup is therefore narrow: within a single
connection, two *distinct* messages with identical bytes that are both
in flight within the dedup window will be conflated as duplicates. With an
8-byte digest and a bounded in-flight window of `N` messages (typically
`send_hwm`), the hash-collision probability is ≈ `N² / 2⁶⁵` — negligible in
practice. The real concern is intentional byte-identical traffic: if the
application sends two distinct logical events with the same payload (e.g.
two separate "increment counter" commands) and expects both deliveries to
be observed, the second will be silently dropped as a duplicate.

This specification deliberately does **not** add a built-in nonce or
sequence number to make every message unique on the wire. Doing so would
duplicate work the application already does when it has uniqueness
requirements, and penalise the common case where byte-identical payloads
truly are duplicates (which is the whole point of dedup). Applications that
need byte-identical messages to be treated as distinct events SHOULD
include an application-level nonce in the payload itself — for example, a
UUID frame, a monotonic counter, or a timestamp with sufficient resolution.
The content hash then distinguishes them naturally. Applications that want
no dedup at all MAY use QoS 1, where hashes serve only for `ACK` correlation
and the receiver performs no dedup.

QoS 3 layers application-level completion on top of this 3-way exchange by
replacing `ACK` with `COMP` / `NACK` — an application-driven analogue of
MQTT's `PUBCOMP`, with no corresponding `PUBREL` leg for the reasons above.

## QoS Levels

| Level | Name                     | Guarantee                                                  |
|-------|--------------------------|------------------------------------------------------------|
| 0     | Fire-and-forget          | None (standard ZMQ behavior)                               |
| 1     | At-least-once            | Sender retries until ACK received                          |
| 2     | Exactly-once             | Like 1, but sender pins to connection — no failover        |
| 3     | Exactly-once + processed | Like 2, plus application-level COMP/NACK on success/error  |

Implementations MUST reject QoS values outside the range they support.

## Handshake

### X-QoS READY Property

The QoS level is exchanged during the ZMTP handshake as a custom property in
the READY command (or INITIATE command for CURVE clients):

```
Property name:  "X-QoS"
Property value: ASCII decimal string ("0", "1", "2", "3")
```

When QoS is 0 (the default), the X-QoS property SHOULD be omitted to avoid
overhead. If absent, a peer's QoS level MUST be assumed to be 0.

### QoS level matching

Both peers MUST advertise the same QoS level. If a peer receives a READY (or
INITIATE) with a different X-QoS value than its own, it MUST close the
connection immediately. Implementations MUST NOT silently fall back to QoS 0.

Rationale: silent degradation masks configuration errors and violates the
application's delivery expectations. A PUSH socket configured for at-least-once
delivery that silently drops to fire-and-forget defeats the purpose of QoS.

### Peer identity (QoS >= 2)

At QoS >= 2, peers MUST be identifiable across reconnects so that the sender
can distinguish "reconnect to the same peer" from "connect to a new peer."

A peer is identifiable if **either** of the following is true:

* The connection uses a CURVE transport mechanism (the peer's long-term public
  key serves as identity).
* The peer has set a non-empty ZMQ_IDENTITY (routing ID).

If neither condition is met, the peer receiving the READY/INITIATE command MUST
close the connection. Implementations MUST NOT fall back to endpoint-based
identity (IP address, DNS name), as this is unreliable across network changes
and load balancers.

### Interoperability with libzmq

libzmq and other ZMTP implementations that do not support this specification
will not send X-QoS, which is equivalent to QoS 0. A QoS >= 1 socket
connecting to a libzmq peer will see a QoS mismatch and drop the connection.
This is intentional — mixing guaranteed and unguaranteed peers would violate
delivery semantics.

## Command Frames

### Wire format

ACK, CLR, COMP, and NACK are standard ZMTP command frames. The command name is
encoded per [23/ZMTP Section 2.1](https://rfc.zeromq.org/spec/23/):

```
Command frame body:
  [1 byte name_length] [N bytes name] [command data]
```

#### ACK command

Acknowledges receipt of a message at the transport layer. Sent by receiver to
sender.

```
Name: "ACK" (3 bytes)
Data: [1 byte algorithm] [8 bytes hash_digest]
```

Used at QoS 1 and QoS 2. Not used at QoS 3 (replaced by COMP).

#### CLR command

Tells the receiver to remove a digest from its deduplication set. Sent by
sender to receiver after receiving an ACK or COMP.

```
Name: "CLR" (3 bytes)
Data: [1 byte algorithm] [8 bytes hash_digest]
```

Used at QoS >= 2.

#### COMP command

Acknowledges successful application-level processing. Sent by receiver to
sender. Replaces ACK at QoS 3.

```
Name: "COMP" (4 bytes)
Data: [1 byte algorithm] [8 bytes hash_digest]
```

Used at QoS 3 only.

#### NACK command

Signals application-level processing failure. Sent by receiver to sender.

```
Name: "NACK" (4 bytes)
Data: [1 byte algorithm] [8 bytes hash_digest] [error_info]
```

Where `error_info` is:

```
[1 byte error_code] [2 bytes msg_length, big-endian] [msg_length bytes UTF-8 message]
```

The error code byte uses bit 7 as a **retryable flag**, consistent with ZMTP's
use of high bits as flags in frame headers:

```
Error code byte:
  bit 7: 1 = retryable, 0 = terminal
  bits 6-0: error type
```

Predefined error codes:

| Code   | Name       | Retryable | Meaning                        |
|--------|------------|-----------|--------------------------------|
| `0x81` | TIMEOUT    | yes       | Processing timed out           |
| `0x02` | BAD_INPUT  | no        | Message malformed or invalid   |
| `0x83` | INTERNAL   | yes       | Handler crashed / internal error |
| `0x84` | OVERLOADED | yes       | Receiver at capacity           |
| `0x05` | REJECTED   | no        | Explicitly rejected by application |

Implementations MUST respect the retryable bit for unknown error codes. This
allows peers to define custom error codes while ensuring correct retry behavior
from senders that do not recognize them.

Used at QoS 3 only.

### Hash algorithm

This version of the specification mandates **XXH64** as the sole hash
algorithm. The algorithm byte in all command frames MUST be `x` (0x78).

A future revision of this specification MAY introduce additional algorithms
(e.g. XXH128) and a negotiation mechanism. The algorithm byte is retained in
the wire format for forward compatibility.

Implementations MUST reject command frames with an unrecognized algorithm byte.

### Hash input

The hash digest MUST be computed over the **raw ZMTP wire bytes** of the
message (after decryption, if an encrypted transport mechanism is in use),
as produced by encoding each frame with its flags and size header.
This means frame boundaries are part of the digest.

Specifically, for a message with parts `[P0, P1, ..., Pn]`, the hash input is
the concatenation of the ZMTP frame encodings:

```
For each part Pi at index i:
  flags  = 0x01 (MORE) if i < n, else 0x00
  flags |= 0x02 (LONG) if Pi.bytesize > 255

  If LONG flag set:
    wire_frame = [flags:1] [size:8 big-endian] [Pi]
  Else:
    wire_frame = [flags:1] [size:1] [Pi]

hash_input = wire_frame_0 || wire_frame_1 || ... || wire_frame_n
digest     = XXH64(hash_input)
```

**Rationale:** Hashing raw wire bytes (instead of `parts.join("")`) ensures
that messages with different framings but identical concatenated payloads
produce different digests. For example, `["AB", "CD"]` and `["A", "BCD"]`
are distinct messages and MUST produce distinct hashes.

### Digest byte order

The 8-byte XXH64 digest MUST be encoded in **little-endian** byte order
(matching the native output of xxHash on most platforms).

## Per-Socket-Type Behavior

### PUSH/PULL and SCATTER/GATHER

#### QoS 0 (default)

No change from standard behavior.

#### QoS 1

**Sender (PUSH/SCATTER):**

1. Before sending a message, the sender computes its hash and stores the
   message in a **pending store** keyed by digest, then sends the message.
2. The sender MUST listen for incoming ACK command frames on each connection.
3. When an ACK is received, the sender removes the matching entry from the
   pending store.
4. When a connection is lost, the sender MUST re-enqueue all pending messages
   for that connection back into the send queue. They will be delivered to the
   next available peer via round-robin.

**Receiver (PULL/GATHER):**

1. After receiving a message from the recv pump, the receiver computes its hash
   and sends an ACK command back to the sender on the same connection.
2. The ACK is sent **before** the message is delivered to the application. This
   acknowledges receipt at the ZMTP layer, not application processing.

**Retry behavior:**

* Over TCP: Do NOT retry on the same connection. TCP is a reliable stream —
  if the bytes were sent, the kernel will deliver them. Retry only after the
  TCP connection drops (detected by the ACK listener or send pump).
* Over inproc/IPC: Retry after `reconnect_interval` (with exponential backoff).
* Un-ACK'd messages add to the effective HWM, providing natural backpressure
  against traffic amplification.

#### QoS 2

QoS 2 adds **connection pinning** and **receiver-side deduplication** to
prevent the duplicate delivery that QoS 1 causes through failover.

**Sender (PUSH/SCATTER):**

1. Same as QoS 1: compute hash, store in pending store, send message.
2. The pending store is **per-connection**: entries record which connection the
   message was sent on.
3. When an ACK is received, the sender sends a **CLR** command back to the
   receiver on the same connection, then removes the entry from the pending
   store.
4. When a connection is lost, pending messages for that connection MUST NOT be
   re-enqueued to other peers. They remain pending until the **same peer**
   reconnects (identified by CURVE public key or ZMQ_IDENTITY).
5. On reconnect to the same peer, the sender retransmits all pending messages
   for that peer in their original order, before sending any new messages.
6. If the peer does not reconnect within the configured **dead-letter timeout**,
   pending messages are dead-lettered (see [Dead letter](#dead-letter)).

**Receiver (PULL/GATHER):**

1. The receiver maintains a **deduplication set** of digests for messages it has
   already delivered.
2. On receiving a message, the receiver computes its hash and checks the dedup
   set:
   - If the digest is **not** in the set: add it, send ACK, deliver to the
     application.
   - If the digest **is** in the set (retransmit after reconnect): send ACK,
     do NOT deliver again.
3. When a CLR command is received, the receiver removes the corresponding digest
   from the dedup set.
4. The dedup set SHOULD have a **TTL** on entries (default: 60 seconds) to
   prevent unbounded growth if CLR commands are lost. Entries SHOULD also be
   evicted when the set exceeds `recv_hwm` entries (oldest first).

#### QoS 3

QoS 3 replaces transport-layer ACK with **application-level confirmation**. The
receiver tells the sender whether the message was successfully processed.

**Sender (PUSH/SCATTER):**

1. Same as QoS 2: compute hash, store in per-connection pending store, send
   message.
2. The sender listens for **COMP** and **NACK** command frames (not ACK).
3. On COMP: send CLR, remove from pending store. Delivery complete.
4. On NACK:
   - If the error code is **retryable** (bit 7 set): re-send the message to
     the same peer after a backoff delay. Increment a retry counter.
   - If the error code is **terminal** (bit 7 clear): dead-letter the message
     immediately.
   - If the retry counter exceeds the configured **max retries** (default: 3):
     dead-letter the message.
5. Connection loss behavior is the same as QoS 2 (pin to same peer, no
   failover).

**Receiver (PULL/GATHER):**

1. The receiver maintains a dedup set as at QoS 2.
2. On receiving a message, the receiver checks the dedup set for duplicates
   (same as QoS 2). If not a duplicate, the message is delivered to the
   application and the receiver **waits for the application to signal
   completion**.
3. On success: the receiver sends a **COMP** command.
4. On failure: the receiver sends a **NACK** command with the appropriate error
   code and a human-readable error message.
5. If the application does not signal within the configured **processing
   timeout**, the receiver sends a NACK with error code `0x81` (TIMEOUT).
6. On CLR: remove from dedup set (same as QoS 2).

**Application processing interface:**

The mechanism by which the application signals completion or failure is
implementation-defined. Languages with exception handling MAY use block-based
processing where COMP is sent on normal return and NACK on exception. Languages
without exceptions SHOULD provide explicit completion and rejection functions.

### REQ/REP

#### QoS 0 (default)

No change from standard behavior.

#### Send/recv ordering

REQ sockets MUST enforce strict send/recv/send/recv alternation regardless of
QoS level. A REQ socket maintains a state flag:

* `:ready` — a send is allowed
* `:waiting_reply` — a receive is expected

Calling send while in `:waiting_reply` state, or receive while in `:ready`
state, MUST raise an error. The recv pump flips the state back to `:ready`
when a reply is delivered.

#### QoS 1

REQ/REP does not use ACK/NACK command frames. **The reply IS the
acknowledgment.**

At QoS 1, if the connection drops while in `:waiting_reply` state:

1. The REQ socket flips back to `:ready`.
2. The original request is re-enqueued to the send queue.
3. It is delivered to the next REP peer via round-robin.

This makes REQ/REP production-usable. Standard ZMQ REQ/REP can get "stuck"
when a REP peer dies between receiving the request and sending the reply.
With QoS 1, the REQ transparently retries on the next REP.

**Applications SHOULD ensure request handlers are idempotent**, as the same
request may be delivered to multiple REP peers.

REP sockets require no QoS-specific changes — replying is their normal
behavior.

#### QoS 2

At QoS 2, the reply is still the acknowledgment, but **failover is disabled**.

If the connection drops while in `:waiting_reply` state:

1. The REQ socket remains in `:waiting_reply` state.
2. The original request remains pending for the **same REP peer** (identified
   by CURVE public key or ZMQ_IDENTITY).
3. On reconnect to the same peer, the request is retransmitted.
4. If the peer does not reconnect within the configured dead-letter timeout, the
   request is dead-lettered and the REQ socket transitions to `:ready`.

**Applications SHOULD still ensure request handlers are idempotent.** While
QoS 2 prevents cross-peer duplicates, a same-peer retransmit after reconnect
may cause the REP to process the request a second time (e.g. if the REP
processed the request but the connection dropped before the reply was sent).

REQ/REP does not use CLR at QoS 2. The reply completes the exchange; there is
no dedup set on the REP side.

#### QoS 3

At QoS 3, REQ/REP adds **application-level error signaling** via NACK. The
reply continues to serve as the success signal (COMP is not used separately).

**REP behavior:**

* On success: send the reply normally. The reply serves as COMP.
* On failure: send a **NACK command frame** instead of a reply. The NACK
  contains the error code and human-readable message. No data frames are sent.

**REQ behavior:**

* If reply data frames arrive: success. Deliver to the application normally.
* If a NACK command frame arrives: failure. The REQ socket MUST make the error
  code and message available to the application through an
  implementation-defined mechanism (e.g. raising an exception from `recv`,
  returning an error object, or providing a separate status query function).
  The REQ socket transitions to `:ready`.

The REQ application decides whether to retry — the library does not auto-retry
for REQ/REP, as the application has context the library lacks (e.g. whether to
retry with the same arguments, modify the request, or give up).

Connection loss behavior is the same as QoS 2 (pin to same REP, no failover).

### Excluded socket types: PUB/SUB, XPUB/XSUB, RADIO/DISH

Fan-out socket types MUST reject any QoS level greater than 0 during the
handshake.

At-least-once delivery is conceptually meaningless for fan-out: each subscriber
receives its own copy of every published message, so there is no other peer to
fail over to when a subscriber disconnects. The other subscribers already have
their copies; the dropped subscriber's copy is simply gone. Adding ACKs to
fan-out would provide per-subscriber backpressure plumbing without delivering
the guarantee the QoS level claims to offer.

True fan-out reliability requires per-subscriber durable queues, replay
protocols, and stable subscriber identity across reconnects. These belong to
broker-style middleware, not to a per-hop transport extension.

## Dead Letter

At QoS >= 2, messages can become undeliverable when a pinned peer does not
reconnect or when retry limits are exhausted (QoS 3). These messages are
**dead-lettered**.

Dead-lettering occurs when:

* **QoS 2/3, connection loss:** The pinned peer does not reconnect within the
  configured dead-letter timeout. Default: implementation-defined, RECOMMENDED
  60 seconds.
* **QoS 3, terminal NACK:** The receiver sends a NACK with a terminal error
  code (bit 7 clear). The message is dead-lettered immediately.
* **QoS 3, retry exhaustion:** The retry counter exceeds the configured max
  retries. The message is dead-lettered.

The dead-letter mechanism is implementation-defined. Implementations SHOULD
provide a callback or event that delivers the dead-lettered message and the
reason (timeout, terminal error, retry exhaustion) to the application.
Implementations that do not provide a callback MUST log a warning when a
message is dead-lettered.

## Inproc Transport

### Command queues

At QoS >= 1, inproc connections MUST have command queues for ACK/CLR/COMP/NACK
flow, even if the socket types would not normally require command support.

### DirectPipe bypass

The DirectPipe optimization (single-peer inproc bypass that skips ZMTP framing)
MUST be disabled at QoS >= 1. The recv pump is where ACK commands are sent, and
the DirectPipe bypass skips the recv pump.

## HWM Interaction

Pending (un-ACK'd) messages are "in flight" — they have left the send queue but
have not been confirmed. Implementations MAY count pending messages toward the
send HWM to provide backpressure.

At QoS 3, messages may remain pending for the duration of application-level
processing, which can be significantly longer than network round-trip time.
Implementations SHOULD count pending messages toward the send HWM at QoS 3 to
prevent the sender from overwhelming a slow receiver.

At QoS 1 and 2, the first version of this specification treats pending messages
as out-of-band (similar to TCP kernel buffers) and does not count them toward
HWM. This may be revised in future versions based on operational experience.

## xxHash

This specification uses [xxHash](https://github.com/Cyan4973/xxHash) by Yann
Collet (the creator of LZ4 and Zstandard). xxHash is chosen for:

* **Speed.** xxHash is the fastest general-purpose hash function family across all
  message sizes, from small (< 64 bytes) to large (> 1 MB).
* **Quality.** xxHash passes all tests in SMHasher and has excellent avalanche
  properties for a non-cryptographic hash.
* **Availability.** xxHash implementations exist for C, C++, Rust, Go, Python,
  Ruby, Java, and many other languages.

### Reference implementation

The reference hash computation (Ruby):

```ruby
require "xxhash"

# parts: Array of frozen binary Strings (message frames)
wire_bytes = Protocol::ZMTP::Codec::Frame.encode_message(parts)
digest     = [XXhash.xxh64(wire_bytes)].pack("Q<")  # 8 bytes, little-endian
```

## Security Considerations

* **xxHash is not cryptographic.** It does not protect against intentional
  collision attacks. An adversary who can inject messages could craft two
  different messages with the same XXH64 digest, causing a false ACK. To
  mitigate this, use a secure transport (TLS, CURVE) so that only authenticated
  peers can send messages.

* **Collision probability.** With 64-bit digests and `N` messages in flight,
  the probability of an accidental collision is approximately `N² / 2⁶⁵`. With
  1000 in-flight messages, this is ~5.4 × 10⁻¹⁴. Applications with strict
  correctness requirements SHOULD add application-level sequence numbers.

* **Dedup set collisions (QoS >= 2).** At QoS 2 and 3, the receiver maintains a
  deduplication set of delivered digests. A hash collision in this set would
  cause a genuinely new message to be silently suppressed as a "duplicate." The
  probability is the same as above — negligible for realistic workloads — but the
  consequence is message loss rather than a false ACK. Applications with strict
  correctness requirements SHOULD add application-level sequence numbers.

* **Replay.** QoS 1 provides at-least-once delivery, not exactly-once.
  Applications MUST handle duplicate messages. QoS 2 and 3 provide
  exactly-once delivery within a session, but across peer crashes (where
  in-memory state is lost), duplicates may still occur. Adding a sequence
  number or unique ID to the message payload is the standard approach for
  end-to-end deduplication.

* **Amplification.** A misbehaving peer that never ACKs could cause unbounded
  growth in the sender's pending store. Implementations SHOULD limit the
  pending store size (e.g. to `send_hwm`) and either block or drop messages
  when the limit is reached.
