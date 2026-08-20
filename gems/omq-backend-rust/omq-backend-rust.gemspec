# frozen_string_literal: true

require_relative "lib/omq/rust/version"

Gem::Specification.new do |s|
  s.name     = "omq-backend-rust"
  s.version  = OMQ::Backend::Rust::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "OMQ.rs backend for OMQ.rb"
  s.description = "Drop-in Rust backend for OMQ. Same socket API (REQ/REP, " \
                  "PUB/SUB, PUSH/PULL, DEALER/ROUTER, and all draft types), " \
                  "backed by the first-class omq-rs Ruby binding. Fully " \
                  "interoperable with the default Ruby engine."
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-backend-rust"
  s.license  = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir[
    "lib/**/*.rb",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
  ]
  s.require_paths = ["lib"]

  s.add_dependency "omq", "~> 0.28"
  s.add_dependency "omq-rs", "~> 0.1"
end
