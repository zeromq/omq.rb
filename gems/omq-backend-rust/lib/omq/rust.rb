# frozen_string_literal: true

require "omq"
require "omq/rust/omq_backend_rust"
require_relative "rust/version"
require_relative "rust/engine"

module OMQ
  module Rust
    @io_threads = 1

    class << self
      attr_accessor :io_threads
    end
  end
end

OMQ::Backend.register(:rust, OMQ::Rust::Engine)
