# frozen_string_literal: true

require_relative "test_helper"
require "rbconfig"

class RequirePathsTest < Minitest::Test
  ROOT     = File.expand_path("../../..", __dir__)
  GEM_ROOT = File.expand_path("..", __dir__)

  def test_backend_libzmq_require_path
    skip "libzmq not available" unless OMQ_LIBZMQ_AVAILABLE

    code = <<~RUBY
      require "omq/backend/libzmq"
      abort "libzmq backend not registered" unless OMQ::Backend.registered?(:libzmq)
      abort "ffi backend registered" if OMQ::Backend.registered?(:ffi)
    RUBY

    assert system(RbConfig.ruby, "-I#{ROOT}/lib", "-I#{GEM_ROOT}/lib", "-e", code),
           "omq/backend/libzmq did not register only the libzmq backend"
  end


  def test_socket_backend_option_lazy_loads_libzmq_backend
    skip "libzmq not available" unless OMQ_LIBZMQ_AVAILABLE

    code = <<~RUBY
      require "omq"
      begin
        socket = OMQ::PULL.new(backend: :libzmq)
        unless socket.engine.is_a?(OMQ::Backend::Libzmq::Engine)
          abort "wrong libzmq engine"
        end
      ensure
        socket&.close
      end
    RUBY

    assert system(RbConfig.ruby, "-I#{ROOT}/lib", "-I#{GEM_ROOT}/lib", "-e", code),
           "backend: :libzmq did not lazy-load the libzmq backend"
  end


  def test_old_ffi_require_path_is_not_available
    code = <<~RUBY
      begin
        require "omq/ffi"
      rescue LoadError
        exit 0
      end

      abort "omq/ffi unexpectedly loaded"
    RUBY

    assert system(RbConfig.ruby, "--disable-gems", "-I#{GEM_ROOT}/lib", "-e", code),
           "omq/ffi should not be a compatibility alias"
  end


  def test_old_ffi_files_do_not_exist
    assert_empty Dir[File.join(GEM_ROOT, "lib/omq/ffi*")]
  end


  def test_old_ffi_backend_alias_is_not_available
    code = <<~RUBY
      require "omq"
      begin
        OMQ::PULL.new(backend: :ffi)
      rescue ArgumentError
        exit 0
      end

      abort "backend: :ffi unexpectedly worked"
    RUBY

    assert system(RbConfig.ruby, "-I#{ROOT}/lib", "-I#{GEM_ROOT}/lib", "-e", code),
           "backend: :ffi should not be a compatibility alias"
  end
end
