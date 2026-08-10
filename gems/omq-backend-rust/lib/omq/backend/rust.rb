# frozen_string_literal: true

require "omq"
require_relative "../rust/version"

if RUBY_ENGINE == "jruby"
  require_relative "../rust/java/engine"
else
  require_relative "../rust/omq_backend_rust"
  require_relative "../rust/engine"
end

module OMQ
  module Rust
    @io_threads = 1

    class << self
      attr_accessor :io_threads
    end
  end
end

OMQ::Backend.register(:rust, OMQ::Rust::Engine)
