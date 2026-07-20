# frozen_string_literal: true

require_relative "lib/omq/version"

Gem::Specification.new do |s|
  s.name     = "omq"
  s.version  = OMQ::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Pure Ruby ZMQ library"
  s.description = "Pure Ruby implementation of ZeroMQ with all socket types " \
                  "(REQ/REP, PUB/SUB, PUSH/PULL, DEALER/ROUTER, and draft " \
                  "types) and TCP/IPC/inproc transports. Built on protocol-zmtp " \
                  "(ZMTP 3.1 wire protocol) and Async fibers. " \
                  "No native libraries required."
  s.homepage = "https://github.com/zeromq/omq.rb"
  s.license  = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE", "CHANGELOG.md"]

  s.add_dependency "protocol-zmtp", "~> 0.8"
  s.add_dependency "async", "~> 2.38"
  s.add_dependency "io-stream", "~> 0.11"
end
