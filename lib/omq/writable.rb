# frozen_string_literal: true

require "timeout"

module OMQ
  # Pure Ruby Writable mixin. Enqueues messages to the engine's send path.
  #
  module Writable
    include QueueWritable


    # Sends a message.
    #
    # Parts must be String-like (respond to `#to_str`). Use an empty
    # string to send an empty frame — `nil` raises `NoMethodError` so
    # accidental nils surface instead of silently producing a zero-byte
    # frame. Invariants after `#send` returns:
    #
    # * every part is a frozen String
    # * unfrozen String parts are re-tagged to `Encoding::BINARY` in
    #   place (a flag flip, no copy)
    # * the parts array (if the caller passed one) is frozen
    #
    # The receiver always gets frozen `BINARY`-tagged parts — on TCP/IPC
    # via byteslice on the wire, on inproc via {Pipe#send_message} which
    # duplicates the one pathological case (frozen non-BINARY parts) so
    # the receiver sees BINARY like every other transport.
    #
    # @param message [String, #to_str, Array<String, #to_str>]
    # @return [self]
    # @raise [IO::TimeoutError] if write_timeout exceeded
    # @raise [NoMethodError] if a part is not String-like
    #
    def send(message)
      parts = message.is_a?(Array) ? message : [message]
      raise ArgumentError, "message has no parts" if parts.empty?

      parts = parts.map { |p| p.to_str } if parts.any? { |p| !p.is_a?(String) }

      parts.each do |part|
        part.force_encoding(Encoding::BINARY) unless part.frozen? || part.encoding == Encoding::BINARY
        part.freeze
      end
      parts.freeze

      if @engine.on_io_thread?
        Reactor.run(timeout: @options.write_timeout) { @engine.enqueue_send(parts) }
      elsif (timeout = @options.write_timeout)
        if Reactor.native_fiber_scheduler?
          Async::Task.current.with_timeout(timeout, IO::TimeoutError) { @engine.enqueue_send(parts) }
        else
          Reactor.run(timeout:) { @engine.enqueue_send(parts) }
        end
      else
        @engine.enqueue_send(parts)
      end

      self
    end


    # Sends a message (chainable).
    #
    # @param message [String, Array<String>]
    # @return [self]
    #
    def <<(message)
      send(message)
    end


    # Waits until the socket is writable.
    #
    # @param timeout [Numeric, nil] timeout in seconds
    # @return [true]
    #
    def wait_writable(timeout = @options.write_timeout)
      true
    end

  end
end
