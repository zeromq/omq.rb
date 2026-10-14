# frozen_string_literal: true

require "async"
require "minitest/spec"

# Async returns a failed task instead of raising its exception. Wait for root
# test tasks so assertions and timeouts reach Minitest. Child tasks retain their
# normal scheduling and can be awaited by the test when needed.
module AsyncSpecSupport
  def Async(...)
    root = !::Async::Task.current?
    task = super
    task.wait if root
    task
  end
end

Minitest::Spec.prepend(AsyncSpecSupport)
