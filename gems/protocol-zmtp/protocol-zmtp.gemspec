# frozen_string_literal: true

require_relative "lib/protocol/zmtp/version"

Gem::Specification.new do |s|
  s.name     = "protocol-zmtp"
  s.version  = Protocol::ZMTP::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "ZMTP 3.1 wire protocol codec and connection"
  s.description = "Pure Ruby implementation of the ZMTP 3.1 wire protocol " \
                  "(ZeroMQ Message Transport Protocol). Includes frame codec, " \
                  "greeting, commands, NULL and CURVE mechanisms, and connection " \
                  "management. No runtime dependencies."
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/protocol-zmtp"
  s.license  = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE", "CHANGELOG.md"]
end
