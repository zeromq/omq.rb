# frozen_string_literal: true

require "minitest/autorun"
require "rbconfig"

require_relative "../lib/omq/rust/java/platform"

class RequirePathsTest < Minitest::Test
  ROOT     = File.expand_path("../../..", __dir__)
  GEM_ROOT = File.expand_path("..", __dir__)

  def test_backend_rust_require_path
    assert_require_registers_backend "omq/backend/rust"
  end


  def test_old_rust_require_path_is_not_available
    code = <<~RUBY
      begin
        require "omq/rust"
      rescue LoadError
        exit 0
      end

      abort "omq/rust unexpectedly loaded"
    RUBY

    assert system(RbConfig.ruby, "--disable-gems", "-I#{GEM_ROOT}/lib", "-e", code),
           "omq/rust should not be a compatibility alias"
  end


  def test_socket_backend_option_lazy_loads_rust_backend
    code = <<~RUBY
      require "omq"
      begin
        socket = OMQ::PULL.new(backend: :rust)
        abort "rust backend not registered" unless OMQ::Backend.registered?(:rust)
      ensure
        socket&.close
      end
    RUBY

    assert system(RbConfig.ruby, "-I#{ROOT}/lib", "-I#{GEM_ROOT}/lib", "-e", code),
           "backend: :rust did not lazy-load the rust backend"
  end


  def test_java_platform_gemspec_uses_maven_artifact
    spec = Gem::Specification.load(File.join(GEM_ROOT, "omq-backend-rust-java.gemspec"))

    assert_equal "java", spec.platform.to_s
    assert_empty spec.extensions
    requirements = OMQ::Rust::OMQ_JAVA_CLASSIFIERS.map do |classifier|
      "jar io.github.paddor:omq-java:#{classifier}, #{OMQ::Rust::OMQ_JAVA_VERSION}"
    end
    assert_equal requirements, spec.requirements.grep(/\Ajar io\.github\.paddor:omq-java:/)
    refute spec.files.any? { |path| path.start_with?("ext/") }
  end


  def test_java_platform_classifier_maps_supported_hosts
    assert_equal "linux-x86_64",
                 OMQ::Rust::Java.classifier_for(os_name: "Linux", os_arch: "amd64")
    assert_equal "macos-aarch64",
                 OMQ::Rust::Java.classifier_for(os_name: "Mac OS X", os_arch: "aarch64")
    assert_equal "macos-x86_64",
                 OMQ::Rust::Java.classifier_for(os_name: "Darwin", os_arch: "x86_64")
    assert_equal "windows-x86_64",
                 OMQ::Rust::Java.classifier_for(os_name: "Windows 11", os_arch: "x64")
  end


  def test_java_platform_classifier_rejects_unsupported_hosts
    error = assert_raises(OMQ::Rust::Java::UnsupportedPlatformError) do
      OMQ::Rust::Java.classifier_for(os_name: "Linux", os_arch: "aarch64")
    end

    assert_match(/unsupported OMQ\.java platform/, error.message)
  end


  def test_jruby_uses_java_engine
    skip "JRuby only" unless RUBY_ENGINE == "jruby"

    require "omq/backend/rust"

    assert_same OMQ::Rust::Java::Engine, OMQ::Backend.fetch(:rust)
  end


  private


  def assert_require_registers_backend(path)
    code = <<~RUBY
      require #{path.dump}
      abort "rust backend not registered" unless OMQ::Backend.registered?(:rust)
    RUBY

    assert system(RbConfig.ruby, "-I#{ROOT}/lib", "-I#{GEM_ROOT}/lib", "-e", code),
           "#{path} did not register the rust backend"
  end
end
