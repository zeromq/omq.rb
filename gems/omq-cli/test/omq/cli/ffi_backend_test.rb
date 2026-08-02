# frozen_string_literal: true

require_relative "../../test_helper"
require "securerandom"

# These tests exercise the --ffi flag end-to-end by running RepRunner
# threads with mixed backends: one REP uses libzmq via the legacy FFI flag,
# the other uses the pure-Ruby engine. A single REQ socket connects to
# both and round-robins requests across them, verifying that replies
# come back correctly regardless of which backend handled each request.
#
# Both REP reply modes are covered:
#   * --echo            (config.echo = true)
#   * --recv-eval EXPR  (config.recv_expr)

def ffi_test_url(label) = "ipc://@omq-ffi-test-#{label}-#{SecureRandom.hex(4)}"


BACKEND_TEST_TIMEOUT = ENV.fetch("OMQ_TEST_TIMEOUT", "30").to_f


def wait_for_connection_count(socket, count)
  deadline = Async::Clock.now + BACKEND_TEST_TIMEOUT
  sleep 0.01 until socket.connection_count >= count || Async::Clock.now >= deadline
  assert_operator socket.connection_count, :>=, count
end


# Spawn a RepRunner with the given backend on its own Async event loop.
def spawn_rep(cfg)
  Thread.new do
    Sync do |task|
      OMQ::CLI::RepRunner.new(cfg, OMQ::REP).call(task)
    end
  end
end


def make_rep_cfg(url:, ffi:, count:, echo: false, recv_expr: nil)
  make_config(
    type_name: "rep",
    endpoints: [OMQ::CLI::Endpoint.new(url, true)],
    binds:     [url],
    echo:      echo,
    recv_expr: recv_expr,
    count:     count,
    quiet:     true,
    ffi:       ffi,
  )
end


describe "REQ -> mixed-backend REPs" do
  before do
    require "omq/backend/libzmq"
  end


  it "round-robins to FFI REP --echo and pure-Ruby REP --echo" do
    pure_url = ffi_test_url("rep-pure-echo")
    ffi_url  = ffi_test_url("rep-ffi-echo")
    n_reqs   = 6  # even number so each REP handles half

    pure_thread = spawn_rep(make_rep_cfg(url: pure_url, ffi: false, echo: true, count: n_reqs / 2))
    ffi_thread  = spawn_rep(make_rep_cfg(url: ffi_url,  ffi: true,  echo: true, count: n_reqs / 2))

    client_thread = Thread.new do
      Sync do
        client = OMQ::REQ.new
        client.linger = 1
        client.recv_timeout = BACKEND_TEST_TIMEOUT
        client.connect(pure_url)
        client.connect(ffi_url)
        wait_for_connection_count(client, 2)

        n_reqs.times.map do |i|
          client.send(["req-#{i}"])
          client.receive&.first
        end
      ensure
        client&.close
      end
    end

    results = client_thread.value
    pure_thread.join
    ffi_thread.join

    expected = n_reqs.times.map { |i| "req-#{i}" }
    assert_equal expected, results
  end


  it "round-robins to FFI REP --recv-eval and pure-Ruby REP --recv-eval" do
    pure_url = ffi_test_url("rep-pure-eval")
    ffi_url  = ffi_test_url("rep-ffi-eval")
    n_reqs   = 6
    upcase   = "it.map(&:upcase)"

    pure_thread = spawn_rep(make_rep_cfg(url: pure_url, ffi: false, recv_expr: upcase, count: n_reqs / 2))
    ffi_thread  = spawn_rep(make_rep_cfg(url: ffi_url,  ffi: true,  recv_expr: upcase, count: n_reqs / 2))

    client_thread = Thread.new do
      Sync do
        client = OMQ::REQ.new
        client.linger = 1
        client.recv_timeout = BACKEND_TEST_TIMEOUT
        client.connect(pure_url)
        client.connect(ffi_url)
        wait_for_connection_count(client, 2)

        n_reqs.times.map do |i|
          client.send(["req-#{i}"])
          client.receive&.first
        end
      ensure
        client&.close
      end
    end

    results = client_thread.value
    pure_thread.join
    ffi_thread.join

    expected = n_reqs.times.map { |i| "REQ-#{i}" }
    assert_equal expected, results
  end
end
