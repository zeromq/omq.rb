# frozen_string_literal: true

require_relative "lib/omq/rust/version"

Gem::Specification.new do |s|
  s.name     = "omq-backend-rust"
  s.version  = OMQ::Rust::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Rust-backed engine for OMQ using OMQ.java on JRuby"
  s.description = "Drop-in Rust backend for OMQ on JRuby. Same socket API " \
                  "(REQ/REP, PUB/SUB, PUSH/PULL, DEALER/ROUTER, and all " \
                  "draft types), but networking runs through the OMQ.java " \
                  "Maven artifact."
  s.homepage = "https://github.com/zeromq/omq.rb/tree/main/gems/omq-backend-rust"
  s.license  = "ISC"
  s.platform = "java"

  s.required_ruby_version = ">= 3.3"

  s.files = [
    "lib/omq/backend/rust.rb",
    "lib/omq/rust/version.rb",
    *Dir["lib/omq/rust/java/**/*.rb"].sort,
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
  ]
  s.require_paths = ["lib"]

  s.add_dependency "omq", "~> 0.28"
  s.add_dependency "jar-dependencies", ">= 0.5.7"
  OMQ::Rust::OMQ_JAVA_CLASSIFIERS.each do |classifier|
    s.requirements << "jar io.github.paddor:omq-java:#{classifier}, #{OMQ::Rust::OMQ_JAVA_VERSION}"
  end
end
