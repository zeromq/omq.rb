# frozen_string_literal: true

# OMQ LZ4+TCP transport — adds lz4+tcp:// endpoint support.
#
# Usage:
#   require "omq/transport/lz4_tcp"
#
#   push = OMQ::PUSH.new
#   push.connect("lz4+tcp://127.0.0.1:5555", dict: File.binread("my.dict"))
#   # or without a dictionary:
#   push.connect("lz4+tcp://127.0.0.1:5555")

require "omq"
require "lz4rip"

require_relative "../lz4/errors"
require_relative "../lz4/codec"
require_relative "lz4_tcp/transport"
