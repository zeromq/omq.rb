# frozen_string_literal: true

require "minitest/autorun"
require "rbconfig"

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
