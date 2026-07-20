# frozen_string_literal: true

require_relative "lib/omq/transport/websocket/version"

Gem::Specification.new do |s|
  s.name        = "omq-websocket"
  s.version     = OMQ::Transport::WebSocket::VERSION
  s.authors     = ["Patrik Wenger"]
  s.email       = ["paddor@gmail.com"]
  s.summary     = "ZeroMQ-over-WebSocket transport for OMQ (ws:// and wss://)"
  s.description = "Adds ws:// and wss:// transports to OMQ, implementing " \
                  "ZeroMQ RFC 45 (ZWS 2.0). Built on async-websocket. " \
                  "Registers both schemes on require; no OMQ core changes " \
                  "required beyond the .connection_class hook (omq >= 0.27)."
  s.homepage    = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-websocket"
  s.license     = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE", "CHANGELOG.md"]

  s.add_dependency "omq",             "~> 0.28"
  s.add_dependency "protocol-zmtp",   "~> 0.8"
  s.add_dependency "async",           "~> 2.38"
  s.add_dependency "async-http",      "~> 0.94"
  s.add_dependency "async-websocket", "~> 0.30"
end
