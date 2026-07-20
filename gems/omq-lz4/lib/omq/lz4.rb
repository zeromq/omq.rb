# frozen_string_literal: true

# OMQ LZ4+TCP transport — adds lz4+tcp:// endpoint support.
#
# Complementary to `omq-zstd`: LZ4 block format has no entropy stage
# (no Huffman, no FSE), ~16 KiB of encoder state per connection, and
# trades worse compression ratio for ~4–8× faster encode and ~3× less
# memory. Pick `lz4+tcp://` for CPU- or memory-scarce deployments where
# the bandwidth savings of zstd aren't worth the per-message CPU.
#
# See RFC.md for the wire format (not yet written — scheme is still
# under development).

require_relative "lz4/version"
require_relative "lz4/errors"
require_relative "lz4/codec"
require_relative "transport/lz4_tcp"
