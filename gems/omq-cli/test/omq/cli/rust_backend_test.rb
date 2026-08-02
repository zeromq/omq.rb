# frozen_string_literal: true

require_relative "../../test_helper"
require "open3"
require "rbconfig"
require "tempfile"
require "timeout"
require "tmpdir"


RUST_BACKEND_TEST_TIMEOUT = ENV.fetch("OMQ_TEST_TIMEOUT", "30").to_f


def rust_backend_test_url(label)
  path = File.join(Dir.tmpdir, "omq-cli-rust-#{label}-#{SecureRandom.hex(4)}.sock")
  ["ipc://#{path}", path]
end


def omq_exe(*args)
  root = File.expand_path("../../..", __dir__)
  [RbConfig.ruby, "-I#{File.join(root, "lib")}", File.join(root, "exe/omq"), *args]
end


def wait_for_socket_path(path, stderr_file)
  deadline = Async::Clock.now + RUST_BACKEND_TEST_TIMEOUT
  sleep 0.01 until File.socket?(path) || Async::Clock.now >= deadline
  assert File.socket?(path), "REP did not bind #{path}\n#{stderr_file.read}"
end


def wait_process(pid)
  Timeout.timeout(RUST_BACKEND_TEST_TIMEOUT) { Process.wait2(pid) }
rescue Timeout::Error
  Process.kill("TERM", pid)
  Timeout.timeout(RUST_BACKEND_TEST_TIMEOUT) { Process.wait2(pid) }
end


def stop_process(pid)
  Process.kill("TERM", pid)
rescue Errno::ESRCH
ensure
  begin
    Process.wait(pid)
  rescue Errno::ECHILD
  end
end


describe "CLI Rust backend" do
  it "round-trips a REP echo through exe/omq" do
    url, path = rust_backend_test_url("rep-echo")
    server_out = Tempfile.new("omq-cli-rust-out")
    server_err = Tempfile.new("omq-cli-rust-err")

    pid = Process.spawn(
      *omq_exe("rep", "--backend", "rust", "-b", url, "--echo", "-n", "1", "-q", "-t", RUST_BACKEND_TEST_TIMEOUT.to_s),
      out: server_out.path,
      err: server_err.path,
    )

    wait_for_socket_path(path, server_err)

    stdout, stderr, status = Open3.capture3(
      *omq_exe("req", "--backend", "rust", "-c", url, "-t", RUST_BACKEND_TEST_TIMEOUT.to_s),
      stdin_data: "hello\n",
    )
    _, server_status = wait_process(pid)

    assert status.success?, stderr
    server_err.rewind
    assert server_status.success?, server_err.read
    assert_equal "hello\n", stdout
  ensure
    stop_process(pid) if pid
    File.unlink(path) if path && File.exist?(path)
    server_out&.close!
    server_err&.close!
  end
end
