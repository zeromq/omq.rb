# frozen_string_literal: true

require_relative "lib/omq/lz4/version"

Gem::Specification.new do |s|
  s.name        = "omq-lz4"
  s.version     = OMQ::LZ4::VERSION
  s.authors     = ["Patrik Wenger"]
  s.email       = ["paddor@gmail.com"]
  s.summary     = "LZ4+TCP transport for OMQ"
  s.description = "Adds lz4+tcp:// endpoint support to OMQ with per-part " \
                  "LZ4 block-format compression, bounded decompression, " \
                  "and in-band dictionary shipping. Complementary to " \
                  "omq-zstd: worse ratio, far faster encode, far smaller " \
                  "per-connection footprint."
  s.homepage    = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-lz4"
  s.license     = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files = Dir["lib/**/*.rb", "README.md", "CHANGELOG.md", "LICENSE"]

  s.add_dependency "omq",    "~> 0.28"
  s.add_dependency "lz4rip", "~> 0.1.1"
end
