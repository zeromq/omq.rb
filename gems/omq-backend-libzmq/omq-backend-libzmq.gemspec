# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name     = "omq-backend-libzmq"
  s.version  = "0.3.1"
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "libzmq backend for OMQ using FFI"
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-backend-libzmq"
  s.license  = "ISC"

  s.required_ruby_version = ">= 3.3"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]

  s.add_dependency "omq", "~> 0.28"
  s.add_dependency "ffi"
end
