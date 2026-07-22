# frozen_string_literal: true

require "minitest/autorun"
require "omq-ractor"
require "async"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false


# A background thread that raises during a test should abort the main
# thread immediately, not leave the test hanging on a receive from a
# dead peer. Minitest prints the exception + backtrace from the aborting
# thread, so the test fails loudly instead of silently timing out.
Thread.abort_on_exception = true


RECONNECT_INTERVAL = 0.01

def wait_connected(*sockets, timeout: 2)
  sockets.each do |s|
    Async::Task.current.with_timeout(timeout) { s.peer_connected.wait }
  end
end
