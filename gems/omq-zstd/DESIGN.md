# Zstd transport architecture

The wire contract is [ZMTP-Zstd revision 2](RFC.md). Compression starts
only after the normal ZMTP handshake. Commands remain uncompressed.

## Send path

Routing applies subscription filters and queue limits to logical messages.
The transport receives the messages admitted for one connection and encodes
isolated or large messages per part. Already queued small messages may share
an arena, targeting 32 KiB without waiting for additional traffic.

A codec owns the send dictionary, training state, and compression settings.
It is shared by the endpoints of one socket. Individual-message encoding
can reuse the result for the same frozen message across subscribers.
Arenas are built from each subscriber's admitted sequence.

Dictionary shipment is tracked per connection and direction. A newly
connected peer receives the dictionary before any data that uses it.
Training is optional, uses a 2 KiB dictionary, and collects up to 1000 samples
or 100 KiB. Samples of 2048 bytes or more are excluded.

## Receive path

Each connection owns its receive dictionary and any unread arena messages.
It accepts raw parts, Zstandard frames, one dictionary shipment, and raw or
compressed `ZSTA` arenas. Dictionary shipments must be single-part messages.

The shared compression transport layer validates an entire arena before
returning its first message. Each subsequent receive returns one logical
message, preserving queue backpressure and receive scheduling. An invalid
arena closes its connection without exposing a valid prefix or killing the
socket's other connections.

## Bounds

- Arena envelope: at most 65552 bytes.
- Decoded arena: at most 65536 bytes, 256 messages, and 4096 parts.
- Dictionary: at most 8192 bytes, shipped once per direction.
- Application size limit: checked per logical message inside an arena,
  including a 64-byte storage charge per part.

Zstandard frames must declare their content size and consume exactly their
wire part. Arena frames must declare the same size as their arena header.
Decompression is bounded by that size. The outer ZMTP framer allows transport
envelopes independently of the application message limit.

Encrypted mechanisms are rejected on compressed endpoints.
