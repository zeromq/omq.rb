# frozen_string_literal: true

require "omq"
require_relative "../ffi/libzmq"
require_relative "../ffi/engine"

OMQ::Backend.register(:libzmq, OMQ::FFI::Engine)
OMQ::Backend.register(:ffi, OMQ::FFI::Engine)
