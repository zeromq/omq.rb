# frozen_string_literal: true

require "minitest/autorun"
require "omq/qos"
require "async"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

RECONNECT_INTERVAL = 0.01

def wait_connected(*sockets, timeout: 2)
  sockets.each do |s|
    Async::Task.current.with_timeout(timeout) { s.peer_connected.wait }
  end
end
