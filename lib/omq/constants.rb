# frozen_string_literal: true

require "socket"
require "io/stream"

module OMQ
  # When OMQ_DEBUG is set, silent rescue clauses print the exception
  # to stderr so transport/engine bugs surface immediately.
  DEBUG = !!ENV["OMQ_DEBUG"]


  # Raised when an internal pump task crashes unexpectedly.
  # The socket is no longer usable; the original error is available via #cause.
  #
  class SocketDeadError < RuntimeError
  end


  # Lifecycle event emitted by {Socket#monitor}.
  #
  # @!attribute [r] type
  #   @return [Symbol] event type (:listening, :connected, :disconnected, etc.)
  # @!attribute [r] endpoint
  #   @return [String, nil] the endpoint involved
  # @!attribute [r] detail
  #   @return [Hash, nil] extra context (e.g. { error: }, { interval: }, etc.)
  #
  MonitorEvent = Data.define(:type, :endpoint, :detail) do
    def initialize(type:, endpoint: nil, detail: nil) = super
  end


  # Errors raised when a peer disconnects or resets the connection.
  # Not frozen at load time — transport plugins append to this before
  # the first bind/connect, which freezes both arrays.
  CONNECTION_LOST = [
    EOFError,
    IOError,
    Errno::EPIPE,
    Errno::ECONNRESET,
    Errno::ECONNABORTED,
    Errno::ENOTCONN,
    IO::Stream::ConnectionResetError,
  ]


  # Errors raised when a peer cannot be reached.
  CONNECTION_FAILED = [
    Errno::ECONNREFUSED,
    Errno::ENOENT,
    Errno::ETIMEDOUT,
    Errno::EHOSTUNREACH,
    Errno::ENETUNREACH,
    Errno::EPROTOTYPE, # IPC: existing socket file is SOCK_DGRAM, not SOCK_STREAM
    Socket::ResolutionError,
  ]


  # Freezes module-level state so OMQ sockets can be used inside Ractors.
  # Call this once before spawning any Ractors that create OMQ sockets.
  #
  def self.freeze_for_ractors!
    ::Ractor.make_shareable(CONNECTION_LOST)
    ::Ractor.make_shareable(CONNECTION_FAILED)
    Backend.freeze_for_ractors!
    ::Ractor.make_shareable(Engine.transports)
    ::Ractor.make_shareable(Routing.registry)
  end
end
