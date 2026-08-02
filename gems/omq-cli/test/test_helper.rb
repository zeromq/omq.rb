# frozen_string_literal: true

Warning[:experimental] = false

require "minitest/autorun"
require "json"
require "stringio"
require "securerandom"
require "msgpack"
require "async"
require "console"

require "omq"
require "omq/client_server"
require "omq/radio_dish"
require "omq/scatter_gather"
require "omq/channel"
require "omq/peer"
require "omq/zstd"
require "omq/lz4"

require_relative "../lib/omq/cli"

Console.logger = Console::Logger.new(Console::Output::Null.new)

# A background thread that raises during a test should abort the main
# thread immediately, not leave the test hanging on a receive from a
# dead peer. Minitest prints the exception + backtrace from the aborting
# thread, so the test fails loudly instead of silently timing out.
Thread.abort_on_exception = true


# Unique IPC abstract address per call to avoid cross-test interference.
def ipc_url(label) = "ipc://@omq-test-#{label}-#{SecureRandom.hex(4)}"


# Suppress stderr/stdout from abort/puts during validation tests.
def quietly
  orig_stderr = $stderr
  orig_stdout = $stdout
  $stderr = StringIO.new
  $stdout = StringIO.new
  yield
ensure
  $stderr = orig_stderr
  $stdout = orig_stdout
end


# Silence stderr only (keep $stdout intact for tests that assert on it).
def silence_stderr
  orig = $stderr
  $stderr = StringIO.new
  yield
ensure
  $stderr = orig
end


# Helper to build a minimal Config for unit tests.
def make_config(type_name:, **overrides)
  defaults = {
    type_name:        type_name,
    endpoints:        [],
    connects:         [],
    binds:            [],
    in_endpoints:     [],
    out_endpoints:    [],
    data:             nil,
    file:             nil,
    format:           :ascii,
    subscribes:       [],
    joins:            [],
    group:            nil,
    identity:         nil,
    target:           nil,
    interval:         nil,
    count:            nil,
    delay:            nil,
    timeout:          nil,
    linger:           5,
    reconnect_ivl:    nil,
    heartbeat_ivl:    nil,
    send_hwm:         nil,
    recv_hwm:         nil,
    sndbuf:           nil,
    rcvbuf:           nil,
    conflate:         false,
    compress:         nil,
    compress_level:   nil,
    in_compress:        nil,
    in_compress_level:  nil,
    out_compress:       nil,
    out_compress_level: nil,
    send_expr:        nil,
    recv_expr:        nil,
    parallel:         nil,
    transient:        false,
    verbose:          0,
    timestamps:       nil,
    quiet:            false,
    echo:             false,
    scripts:          [],
    recv_maxsz:       nil,
    curve_server:     false,
    curve_server_key: nil,
    crypto:           nil,
    backend:          :ruby,
    ffi:              false,
    stdin_is_tty:     true,
  }

  OMQ::CLI::Config.new(**defaults, **overrides)
end
