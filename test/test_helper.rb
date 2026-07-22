# frozen_string_literal: true

require "minitest/autorun"
require "omq"
require "async"

# Silence Async/Console warnings in tests (e.g. unhandled task exceptions
# that are expected during protocol-error and disconnect tests).
require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

# Fast reconnect interval for tests.
# Production default is 0.1s — tests use 0.01s to cut dead time.
RECONNECT_INTERVAL = 0.01

# Hard per-test timeout: any Async block that runs longer than this
# raises Async::TimeoutError and fails the test immediately.
TEST_ASYNC_TIMEOUT = 1

module Kernel
  alias_method :_Async_base, :Async
  private :_Async_base

  # Hard per-test timeout wrapper. Only applied to Async blocks running
  # on the main thread — OMQ's Reactor runs its own long-lived Async
  # loop on a dedicated I/O thread, and wrapping that in a timeout
  # would kill the reactor mid-suite.
  #
  def Async(&block)
    return _Async_base unless block
    return _Async_base(&block) unless Thread.current == Thread.main

    _Async_base do |task|
      task.with_timeout(TEST_ASYNC_TIMEOUT) { block.call(task) }
    end
  end
end

# Waits for every +socket+ to have at least one peer connection.
# Uses each socket's peer_connected promise instead of sleeping.
def wait_connected(*sockets, timeout: 2)
  barrier = Async::Barrier.new

  sockets.each do |s|
    barrier.async { s.peer_connected.wait }
  end

  Async::Task.current.with_timeout(timeout) do
    barrier.wait
  end
ensure
  barrier&.stop
end
