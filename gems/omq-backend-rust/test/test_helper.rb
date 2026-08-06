# frozen_string_literal: true

require "minitest/autorun"
require "omq"
require "omq/backend/rust"
require "async"
require "timeout"

require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

BACKEND = :rust
TRUFFLERUBY_WITHOUT_ASYNC = RUBY_ENGINE == "truffleruby" && !OMQ::Reactor.native_fiber_scheduler?

class ThreadTestTask
  def async
    thread = Thread.new { yield }
    ThreadFuture.new(thread)
  end


  def with_timeout(seconds, &block)
    Timeout.timeout(seconds, Timeout::Error, &block)
  end
end

class ThreadFuture
  def initialize(thread)
    @thread = thread
  end


  def wait
    @thread.value
  end
end

class CompletedTestTask
  def initialize(value)
    @value = value
  end


  def wait
    @value
  end
end

def run_backend
  if TRUFFLERUBY_WITHOUT_ASYNC
    CompletedTestTask.new(yield ThreadTestTask.new)
  else
    Async { |task| yield task }
  end
end

def skip_without_ruby_backend
  skip "Ruby backend requires native Fiber.scheduler" if TRUFFLERUBY_WITHOUT_ASYNC
end

# Default linger to 0 in tests so close() doesn't block.
OMQ::Options.prepend(Module.new do
  def initialize(**)
    super
    self.linger = 0
  end
end)
