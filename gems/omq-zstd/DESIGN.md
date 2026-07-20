# omq-transport-zstd — Implementation Notes

These are implementation details for the `zstd+tcp://` transport plugin.
The wire protocol is specified in [RFC.md](RFC.md); this document covers
the OMQ-specific architecture that doesn't belong in the RFC.

## Why a transport plugin

Compression could live at three layers. Each has a fatal flaw except the
transport layer.

**Socket-level wrapper** (too high). A wrapper sits above routing and
knows nothing about transports. It can't distinguish TCP from IPC or
inproc, so it compresses local connections (pure overhead). It also
can't act on new connections naturally — dict shipping requires
per-connection state, but the wrapper only sees messages after routing
has dispatched them. Reconnect handling requires hooking into
connection lifecycle events, which is awkward from outside.

**ZMTP connection layer** (too low). Embedding compression into each
ZMTP connection means PUB fan-out compresses the same message N times
(once per subscriber connection). The connection layer has no
socket-wide view, so there's no way to share compression work across
connections.

**Transport layer** (right). `zstd+tcp://` makes transport selection
explicit in the endpoint URI. Only TCP connections get compressed. IPC
and inproc are unaffected even on the same socket. Dict lifetime
matches connection lifetime naturally (new connection = new wrapper =
re-ship dict). No negotiation needed — both peers use `zstd+tcp://`.
The Codec is socket-wide (shared across connections via the Dialer or
Listener), so PUB compresses once and reuses the result.

## Architecture

```
Socket         bind("zstd+tcp://...")  /  connect("zstd+tcp://...")
  │                                         │
  ▼                                         ▼
Engine         transport.listener(...)      transport.dialer(...)
  │                → Listener                 → Dialer
  │                  holds Codec               holds Codec
  │                  #wrap_connection           #wrap_connection
  ▼                        │                        │
ConnectionLifecycle        ▼                        ▼
  ready!  ──────►  ZstdConnection(conn, codec)
                   │ #write_message  → compress → ship_dict! → delegate
                   │ #receive_message → decode (per-connection recv_dict)
                   │ #respond_to?(:write_wire) → false (forces fan-out path)
```

### Dialer / Listener

Both are stateful transport objects created by the transport module's
`.dialer` / `.listener` factory methods. They hold the Codec and
implement `#wrap_connection(conn)` which wraps raw ZMTP connections
in a `ZstdConnection`. The Engine stores them in `@dialers` / `@listeners`
(Hash keyed by endpoint) and calls `#wrap_connection` during
`ConnectionLifecycle#ready!`.

Reconnect calls `dialer.connect` directly — no transport lookup or
opts replay needed. The Dialer holds everything.

### Codec (socket-wide)

One Codec per Dialer or Listener. Shared across all connections of
that endpoint. Owns:

- **Compression**: `#compress_parts(parts)` with identity cache
- **Training**: sample collection, `Zrip::DictTrainer`, dict ID patching
- **Send dict**: `#send_dict_bytes` — the trained or user-supplied dict bytes

### ZstdConnection (per-connection)

`SimpleDelegator` wrapping a `Protocol::ZMTP::Connection`. Per-connection
state:

- `@dict_shipped` — whether the dict has been sent on this connection
- `@recv_dict` — the peer's dictionary for decompression

Intercepts `#send_message`, `#write_message`, `#write_messages`,
`#receive_message`. Returns `false` for `respond_to?(:write_wire)` to
force fan-out through `#write_message` (which hits the compression
cache) instead of pre-encoded wire bytes.

## Identity-based compression cache

PUB fan-out sends the same frozen message parts Array to every
subscriber's `#write_message`. The Codec exploits this with an
`Object#equal?` check:

```ruby
def compress_parts(parts)
  return @cached_compressed if parts.equal?(@cached_parts)
  # ... compress ...
  @cached_parts      = parts
  @cached_compressed = compressed.freeze
end
```

`.equal?` is O(1) — same frozen Array object from `freeze_message`.
First subscriber pays the compression cost; subsequent subscribers
get the cached result. Net: one compression per message, N wire
writes.

## Dict shipping order

Training can trigger DURING `#compress_parts` (when the sample
threshold is reached mid-compression). The dict must be shipped
AFTER compression but BEFORE the wire write, so the receiver
has the dict before seeing frames that use it:

```ruby
def write_message(parts)
  compressed = @codec.compress_parts(parts)  # may trigger training
  ship_dict!                                  # ships if newly trained
  __getobj__.write_message(compressed)
end
```

## Training heuristics

- **Sample threshold**: 1000 messages OR 100 KiB of plaintext, whichever first
- **Sample size cap**: frames > 1024 bytes are skipped (dictionaries primarily benefit small frames)
- **Dict capacity**: 8 KiB (conservative; Zstd recommends ~100:1 sample-to-dict ratio)
- **Dict ID patching**: auto-trained dicts get a random ID in the user range (32768..2^31-1) to avoid collisions with Zstd's built-in dict IDs
- **Training failure**: if `Zrip::DictTrainer#train` raises, training is disabled permanently for the socket. No retry.

## Frame dispatch

Three sentinels for per-part decoding:

| Preamble (4 bytes hex) | Meaning |
|---|---|
| `00 00 00 00` | Uncompressed plaintext (part too small or incompressible) |
| `28 B5 2F FD` | Zstd compressed frame (the standard Zstd magic number) |
| `37 A4 30 EC` | Zstd dictionary — install into per-connection recv slot |

Dict frames are single-part ZMTP messages. When all parts in a message
are dict frames, `#decode_parts` returns `nil` and `#receive_message`
loops to get the next real message.

## Budget enforcement

The receiver tracks a per-message decompressed byte budget derived from
`max_message_size`. Each part's declared `Frame_Content_Size` is checked
BEFORE decompression. The budget decreases across parts of a multipart
message, so the total decompressed size can't exceed the limit even if
individual parts are within bounds.

## Constants

```
MAX_DICT_SIZE          = 64 KiB   (reject oversized dicts)
DICT_CAPACITY          = 8 KiB    (training target size)
TRAIN_MAX_SAMPLES      = 1000
TRAIN_MAX_BYTES        = 100 KiB
TRAIN_MAX_SAMPLE_LEN   = 1024     (skip large frames for training)
MIN_COMPRESS_NO_DICT   = 512 B
MIN_COMPRESS_WITH_DICT = 64 B
```
