# frozen_string_literal: true

require "minitest/autorun"
require "omq"
require "omq/rust"
require "async"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

BACKEND = :rust

# Default linger to 0 in tests so close() doesn't block.
OMQ::Options.prepend(Module.new do
  def initialize(**)
    super
    self.linger = 0
  end
end)
