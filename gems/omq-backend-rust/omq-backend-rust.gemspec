# frozen_string_literal: true

require_relative "lib/omq/rust/version"

Gem::Specification.new do |s|
  s.name     = "omq-backend-rust"
  s.version  = OMQ::Rust::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Rust-backed engine for OMQ using omq-tokio"
  s.description = "Drop-in Rust backend for OMQ. Same socket API (REQ/REP, " \
                  "PUB/SUB, PUSH/PULL, DEALER/ROUTER, and all draft types), " \
                  "but networking runs on a Tokio runtime inside a native " \
                  "extension compiled via rb_sys. Fully interoperable with " \
                  "the default Ruby engine."
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-backend-rust"
  s.license  = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rs,rb}",
    "Cargo.toml",
    "ext/**/Cargo.toml",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
  ]
  s.require_paths = ["lib"]
  s.extensions    = ["ext/omq_backend_rust/extconf.rb"]

  s.add_dependency "omq", "~> 0.28"
  s.add_dependency "rb_sys", "~> 0.9"
end
