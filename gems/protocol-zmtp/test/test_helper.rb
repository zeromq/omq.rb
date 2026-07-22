# frozen_string_literal: true

$VERBOSE = nil

require "minitest/autorun"
require "protocol/zmtp"
require "socket"
require "io/stream"
require "async"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
