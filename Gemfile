# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "minitest"
gem "rake"
gem "rake-compiler"
gem "localhost"

# Cross-backend interop tests load libzmq via FFI when available.
gem "ffi", require: false

# CURVE tests use Nuckle (pure Ruby, no libsodium).
# Cross-backend interop tests also use rbnacl when available.
gem "nuckle",        path: ENV["OMQ_DEV"] ? "../nuckle" : nil
gem "protocol-zmtp", path: ENV["OMQ_DEV"] ? "../protocol-zmtp" : nil

gem "omq-backend-libzmq", require: false, path: "gems/omq-backend-libzmq"
gem "omq-backend-rust",   require: false, path: "gems/omq-backend-rust"
gem "omq-qos",            require: false, path: "gems/omq-qos"
gem "omq-websocket",      require: false, path: "gems/omq-websocket"

if Gem::Version.new(RUBY_VERSION) >= Gem::Version.new("4.0")
  gem "lz4rip",     "~> 0.1.1", path: ENV["OMQ_DEV"] ? "../lz4rip" : nil
  gem "omq-lz4",    require: false, path: "gems/omq-lz4"
  gem "omq-ractor", require: false, path: "gems/omq-ractor"
  gem "omq-zstd",   require: false, path: "gems/omq-zstd"
  gem "zrip",       "~> 0.1.1", path: ENV["OMQ_DEV"] ? "../zrip" : nil
end

if ENV["OMQ_DEV"]
  gem "benchmark-ips"
  gem "rbnacl", "~> 7.0"
end
