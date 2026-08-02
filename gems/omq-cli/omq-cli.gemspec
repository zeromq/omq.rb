# frozen_string_literal: true

require_relative "lib/omq/cli/version"

Gem::Specification.new do |s|
  s.name     = "omq-cli"
  s.version  = OMQ::CLI::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "ZeroMQ CLI — pipe, filter, and transform messages from the terminal"
  s.description = "Command-line tool for sending and receiving ZeroMQ messages " \
                  "on any socket type (REQ/REP, PUB/SUB, PUSH/PULL, " \
                  "DEALER/ROUTER, and all draft types). Supports Ruby eval " \
                  "(-e/-E), script handlers (-r), pipe virtual socket with " \
                  "Ractor parallelism, multiple formats (ASCII, JSON Lines, " \
                  "msgpack, Marshal), Zstd/LZ4 compression, and CURVE encryption. " \
                  "Like nngcat from libnng, but with Ruby superpowers."
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-cli"
  s.license  = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files      = Dir["lib/**/*.rb", "exe/*", "README.md", "LICENSE", "CHANGELOG.md"]
  s.bindir     = "exe"
  s.executables = ["omq"]

  s.add_dependency "omq",      "~> 0.28"
  s.add_dependency "ffi"
  s.add_dependency "omq-zstd", "~> 0.4"
  s.add_dependency "omq-lz4",  "~> 0.3"
  s.add_dependency "msgpack"
  s.add_dependency "rbnacl",   "~> 7.0"
end
