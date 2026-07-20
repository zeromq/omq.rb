# frozen_string_literal: true

Gem::Specification.new do |s|
  s.name     = "omq-ractor"
  s.version  = "0.1.6"
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Bridge OMQ sockets into Ruby Ractors for true parallel processing"
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-ractor"
  s.license  = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE"]

  s.add_dependency "omq", "~> 0.28"
end
