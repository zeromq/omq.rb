# frozen_string_literal: true

require "minitest/autorun"
require "omq"
require "async"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

# A background thread that raises during a test should abort the main
# thread immediately, not leave the test hanging on a receive from a
# dead peer. Minitest prints the exception + backtrace from the aborting
# thread, so the test fails loudly instead of silently timing out.
Thread.abort_on_exception = true

OMQ_LIBZMQ_AVAILABLE = begin
  require "omq/backend/libzmq"
  true
rescue LoadError
  false
end
OMQ_FFI_AVAILABLE = OMQ_LIBZMQ_AVAILABLE

# Waits for +socket+ to have at least one peer connection.
def wait_connected(*sockets)
  sockets.flatten.each { |s| s.peer_connected.wait }
end
