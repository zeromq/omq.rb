# frozen_string_literal: true

source "https://rubygems.org"

gemspec

gem "minitest"
gem "rake"
gem "localhost"

# Cross-backend interop tests load libzmq via FFI when available.
gem "ffi", require: false

# CURVE tests use Nuckle (pure Ruby, no libsodium).
# Cross-backend interop tests also use rbnacl when available.
gem "nuckle",        path: ENV["OMQ_DEV"] ? "../nuckle" : nil
gem "protocol-zmtp", path: ENV["OMQ_DEV"] ? "../protocol-zmtp" : nil

if ENV["OMQ_DEV"]
  gem "benchmark-ips"
  gem "rbnacl", "~> 7.0"
  gem "chacha20blake3",         path: "../chacha20blake3"
  gem "omq-blake3zmq",          require: false, path: "../omq-blake3zmq"
  gem "omq-backend-rust",       require: false, path: "../omq-backend-rust"
end
