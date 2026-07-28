# frozen_string_literal: true

require_relative "../test_helper"


describe "OMQ.freeze_for_ractors!" do
  it "allows socket construction inside a Ractor" do
    if Gem::Version.new(RUBY_VERSION) < Gem::Version.new("4.0")
      skip "Ruby 4.0+ Ractor API required"
    end

    OMQ.freeze_for_ractors!

    ractor = ::Ractor.new do
      Sync do
        OMQ::PULL.new.close
      end
      :ok
    end

    assert_equal :ok, ractor.value
  end

  it "allows backend registration after a Ractor freeze" do
    OMQ.freeze_for_ractors!
    OMQ::Backend.register(:test_backend, OMQ::Engine)

    assert OMQ::Backend.registered?(:test_backend)
  end
end
