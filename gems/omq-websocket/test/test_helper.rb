# frozen_string_literal: true

require "minitest/autorun"
require "minitest/spec"
require "async"
require "omq"
require "omq/transport/websocket"

# silence
require "console"
Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

TEST_ASYNC_TIMEOUT = 1

module Kernel
  alias_method :_Async_base, :Async
  private :_Async_base


  def Async(*args, **kwargs, &block)
    return _Async_base(*args, **kwargs) unless block
    return _Async_base(*args, **kwargs, &block) unless Thread.current == Thread.main

    _Async_base(*args, **kwargs) do |task|
      task.with_timeout(TEST_ASYNC_TIMEOUT) { block.call(task) }
    end
  end
end
