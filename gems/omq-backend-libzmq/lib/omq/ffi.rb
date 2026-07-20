# frozen_string_literal: true

# Compatibility load path for the libzmq backend.
#
# Usage:
#   require "omq/ffi"
#   push = OMQ::PUSH.new(backend: :ffi)
#
# Raises LoadError if libzmq is not installed.

require_relative "backend/libzmq"
