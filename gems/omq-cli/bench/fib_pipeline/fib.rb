# frozen_string_literal: true

def fib(n)
  n <= 1 ? n : fib(n - 1) + fib(n - 2)
end
