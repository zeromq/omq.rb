# frozen_string_literal: true

require_relative "lib/omq/zstd/version"

Gem::Specification.new do |s|
  s.name        = "omq-zstd"
  s.version     = OMQ::Zstd::VERSION
  s.authors     = ["Patrik Wenger"]
  s.email       = ["paddor@gmail.com"]
  s.summary     = "Zstd+TCP transport for OMQ"
  s.description = "Adds zstd+tcp:// endpoint support to OMQ with per-frame " \
                  "Zstd compression, bounded decompression, in-band " \
                  "dictionary shipping, and sender-side dictionary training."
  s.homepage    = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-zstd"
  s.license     = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files = Dir["lib/**/*.rb", "README.md", "RFC.md", "DESIGN.md", "LICENSE"]

  s.add_dependency "omq",  "~> 0.28"
  s.add_dependency "zrip", "~> 0.1.1"
end
