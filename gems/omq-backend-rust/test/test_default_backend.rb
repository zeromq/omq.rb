# frozen_string_literal: true

require_relative "test_helper"
require "open3"
require "rbconfig"

describe "default backend" do
  it "uses rust when pure Ruby backend cannot run" do
    skip unless NON_NATIVE_WITHOUT_ASYNC

    push = OMQ::PUSH.new
    assert_instance_of OMQ::Rust::Engine, push.engine
  ensure
    push&.close
  end


  it "auto-requires rust backend when pure Ruby backend cannot run" do
    skip unless NON_NATIVE_WITHOUT_ASYNC

    script = <<~RUBY
      $LOAD_PATH.unshift(#{File.expand_path("../../../lib", __dir__).inspect})
      $LOAD_PATH.unshift(#{File.expand_path("../lib", __dir__).inspect})
      $LOAD_PATH.unshift(#{File.expand_path("../../protocol-zmtp/lib", __dir__).inspect})
      require "omq"
      push = OMQ::PUSH.new
      puts push.engine.class.name
      push.close
    RUBY

    out, err, status = Open3.capture3(RbConfig.ruby, "-e", script)
    assert status.success?, err
    assert_match(/\AOMQ::Rust::(?:Java::)?Engine\z/, out.lines.last&.chomp)
  end


  it "still rejects explicit ruby backend without native Fiber.scheduler" do
    skip unless NON_NATIVE_WITHOUT_ASYNC

    assert_raises(NotImplementedError) do
      OMQ::PUSH.new(backend: :ruby)
    end
  end
end
