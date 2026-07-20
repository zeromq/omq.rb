# frozen_string_literal: true

# OMQ Zstd+TCP transport — adds zstd+tcp:// endpoint support.
#
# Usage:
#   require "omq/zstd"
#
#   push = OMQ::PUSH.new
#   push.connect("zstd+tcp://127.0.0.1:5555", level: -3)

require "omq"
require "zrip"

require_relative "zstd_tcp/codec"
require_relative "zstd_tcp/connection"
require_relative "zstd_tcp/transport"

module OMQ
  module Transport
    module ZstdTcp
      class ProtocolError < StandardError; end
    end
  end
end

OMQ::Engine.transports["zstd+tcp"] = OMQ::Transport::ZstdTcp
