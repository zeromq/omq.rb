# frozen_string_literal: true

require_relative "../../test_helper"
require "timeout"

# -- Parallel execution (-P) ------------------------------------------
#
# FIB expression (iterative, no method definition):
#   n = Integer(it.first); a,b = 0,1; n.times { a,b = b,a+b }; [a.to_s]
#
# fib(1..10) = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55]  sum = 143
#
FIB_EXPR = "n=Integer(it.first);a,b=0,1;n.times{a,b=b,a+b};[a.to_s]".freeze
FIB_1_10 = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55].freeze
PARALLEL_TEST_TIMEOUT = ENV.fetch("OMQ_TEST_TIMEOUT", "30").to_f
PARALLEL_IDLE_TIMEOUT = ENV.fetch("OMQ_TEST_IDLE_TIMEOUT", "2").to_f


def wait_for_parallel_condition(message)
  deadline = Async::Clock.now + PARALLEL_TEST_TIMEOUT
  sleep 0.01 until yield || Async::Clock.now >= deadline
  assert yield, message
end


# Run PipeRunner in a dedicated thread so Ractor#join doesn't block the
# main thread's Async scheduler (which may still exist from prior tests).
def run_pipe_runner(cfg)
  Thread.new do
    Sync do |task|
      OMQ::CLI::PipeRunner.new(cfg).call(task)
    end
  end
end


# Run a BaseRunner subclass in a dedicated thread.
def run_runner(runner_class, cfg, socket_class)
  Thread.new do
    Sync do |task|
      runner_class.new(cfg, socket_class).call(task)
    end
  end
end


describe "pull -P parallel execution" do
  it "receives all messages across parallel workers" do
    url    = ipc_url("pull-parallel")
    n_msgs = 20

    cfg = make_config(
      type_name: "pull",
      endpoints: [OMQ::CLI::Endpoint.new(url, false)],
      parallel:  2,
      timeout:   PARALLEL_IDLE_TIMEOUT,
      count:     n_msgs,
      quiet:     true,
    )

    io_thread = Thread.new do
      Sync do
        src = OMQ::PUSH.new
        src.linger = 1
        src.bind(url)
        src.peer_connected.wait
        n_msgs.times { |i| src.send([i.to_s]) }
      ensure
        src&.close
      end
    end

    runner_thread = run_runner(OMQ::CLI::PullRunner, cfg, OMQ::PULL)
    runner_thread.join
    io_thread.join
  end

  it "round-trips wire-compressed messages through parallel workers" do
    n_msgs = 10

    # PUSH binds zstd+tcp://, PULL runner connects via -z upgrade.
    captured = StringIO.new
    push_ep  = nil

    io_thread = Thread.new do
      Sync do
        src = OMQ::PUSH.new
        src.linger = 1
        push_ep = src.bind("zstd+tcp://127.0.0.1:0").to_s
        src.peer_connected.wait
        n_msgs.times { |i| src.send(["msg-#{i}"]) }
      ensure
        src&.close
      end
    end

    wait_for_parallel_condition("PUSH did not bind zstd endpoint") { push_ep }

    # Give the runner a tcp:// URL — the -z flag upgrades it to zstd+tcp://.
    tcp_ep = push_ep.sub("zstd+tcp://", "tcp://")

    cfg = make_config(
      type_name: "pull",
      endpoints: [OMQ::CLI::Endpoint.new(tcp_ep, false)],
      parallel:  2,
      compress:  :zstd,
      compress_level: -3,
      timeout:   PARALLEL_IDLE_TIMEOUT,
      count:     n_msgs,
    )

    runner_thread = Thread.new do
      orig_stdout = $stdout
      $stdout = captured
      begin
        Sync do |task|
          OMQ::CLI::PullRunner.new(cfg, OMQ::PULL).call(task)
        end
      ensure
        $stdout = orig_stdout
      end
    end

    runner_thread.join
    io_thread.join

    lines = captured.string.lines.map(&:chomp).sort
    expected = n_msgs.times.map { |i| "msg-#{i}" }.sort
    assert_equal expected, lines
  end
end


describe "pull -P with oversized frames" do
  it "does not hang when peer sends messages exceeding max_message_size" do
    silence_stderr do
      url = ipc_url("pull-parallel-maxsz")

      cfg = make_config(
        type_name:  "pull",
        endpoints:  [OMQ::CLI::Endpoint.new(url, false)],
        parallel:   1,
        recv_maxsz: 32,
        quiet:      true,
      )

      runner_done = false

      io_thread = Thread.new do
        Sync do
          src = OMQ::PUSH.new(linger: 2)
          src.bind(url)
          src.peer_connected.wait
          src.send(["x" * 1024])
          wait_for_parallel_condition("PullRunner did not finish") { runner_done }
        ensure
          src&.close
        end
      end

      runner_thread = run_runner(OMQ::CLI::PullRunner, cfg, OMQ::PULL)

      begin
        assert runner_thread.join(PARALLEL_TEST_TIMEOUT), "PullRunner hung after peer sent an oversized frame"
      ensure
        runner_done = true
        io_thread.join
      end
    end
  end
end


describe "pipe with oversized frames" do
  it "sequential pipe exits on oversized frames" do
    silence_stderr do
      in_url  = ipc_url("pipe-maxsz-in")
      out_url = ipc_url("pipe-maxsz-out")

      cfg = make_config(
        type_name:     "pipe",
        in_endpoints:  [OMQ::CLI::Endpoint.new(in_url,  false)],
        out_endpoints: [OMQ::CLI::Endpoint.new(out_url, false)],
        recv_maxsz:    32,
        linger:        0,
      )

      runner_done = false

      io_thread = Thread.new do
        Sync do
          src = OMQ::PUSH.new(linger: 2)
          src.bind(in_url)
          sink = OMQ::PULL.new
          sink.bind(out_url)
          src.peer_connected.wait
          src.send(["x" * 1024])
          wait_for_parallel_condition("PipeRunner did not finish") { runner_done }
        ensure
          src&.close
          sink&.close
        end
      end

      runner_exit = nil
      runner_thread = Thread.new do
        Sync do |task|
          OMQ::CLI::PipeRunner.new(cfg).call(task)
        end
      rescue SystemExit => e
        runner_exit = e.status
      end

      begin
        assert runner_thread.join(PARALLEL_TEST_TIMEOUT), "PipeRunner hung after peer sent an oversized frame"
      ensure
        runner_done = true
        io_thread.join
      end
      assert_equal 1, runner_exit, "PipeRunner should have called exit 1 on protocol error"
    end
  end


  it "parallel pipe worker exits on oversized frames" do
    silence_stderr do
      in_url  = ipc_url("pipe-P-maxsz-in")
      out_url = ipc_url("pipe-P-maxsz-out")

      cfg = make_config(
        type_name:     "pipe",
        in_endpoints:  [OMQ::CLI::Endpoint.new(in_url,  false)],
        out_endpoints: [OMQ::CLI::Endpoint.new(out_url, false)],
        parallel:      1,
        recv_maxsz:    32,
        linger:        0,
      )

      runner_done = false

      io_thread = Thread.new do
        Sync do
          src = OMQ::PUSH.new(linger: 2)
          src.bind(in_url)
          sink = OMQ::PULL.new
          sink.bind(out_url)
          src.peer_connected.wait
          src.send(["x" * 1024])
          wait_for_parallel_condition("parallel PipeRunner did not finish") { runner_done }
        ensure
          src&.close
          sink&.close
        end
      end

      runner_thread = run_pipe_runner(cfg)

      begin
        assert runner_thread.join(PARALLEL_TEST_TIMEOUT), "parallel PipeRunner hung after peer sent an oversized frame"
      ensure
        runner_done = true
        io_thread.join
      end
    end
  end
end


describe "rep -P parallel execution" do
  it "echoes requests back from parallel workers" do
    url    = ipc_url("rep-parallel")
    n_reqs = 10

    cfg = make_config(
      type_name: "rep",
      endpoints: [OMQ::CLI::Endpoint.new(url, false)],
      parallel:  2,
      echo:      true,
      timeout:   PARALLEL_IDLE_TIMEOUT,
      count:     n_reqs,
      quiet:     true,
    )

    io_thread = Thread.new do
      Sync do
        client = OMQ::REQ.new
        client.linger = 1
        client.recv_timeout = PARALLEL_TEST_TIMEOUT
        client.bind(url)
        client.peer_connected.wait

        results = n_reqs.times.map do |i|
          client.send(["req-#{i}"])
          client.receive&.first
        end
        results.sort
      ensure
        client&.close
      end
    end

    runner_thread = run_runner(OMQ::CLI::RepRunner, cfg, OMQ::REP)
    runner_thread.join
    results = io_thread.value
    expected = n_reqs.times.map { |i| "req-#{i}" }.sort
    assert_equal expected, results
  end
end


describe "pipe -P parallel execution" do
  it "routes all messages through workers and produces correct fib results" do
    work_url    = ipc_url("pipe-work")
    results_url = ipc_url("pipe-results")
    n_msgs      = 10

    cfg = make_config(
      type_name:     "pipe",
      in_endpoints:  [OMQ::CLI::Endpoint.new(work_url,    false)],
      out_endpoints: [OMQ::CLI::Endpoint.new(results_url, false)],
      parallel:      2,
      recv_expr:     FIB_EXPR,
      timeout:       PARALLEL_IDLE_TIMEOUT,
    )

    io_thread = Thread.new do
      Sync do
        src  = OMQ::PUSH.new(linger: 1)
        src.bind(work_url)
        sink = OMQ::PULL.new(recv_timeout: PARALLEL_TEST_TIMEOUT)
        sink.bind(results_url)

        src.peer_connected.wait
        (1..n_msgs).each { |n| src.send([n.to_s]) }

        n_msgs.times.map { sink.receive&.first.to_i }.sort
      ensure
        src&.close
        sink&.close
      end
    end

    runner_thread = run_pipe_runner(cfg)
    runner_thread.join
    results = io_thread.value
    assert_equal FIB_1_10, results
  end

  it "applies BEGIN/END blocks once per worker" do
    work_url    = ipc_url("pipe-begin-work")
    results_url = ipc_url("pipe-begin-results")

    expr = "BEGIN{ @s=0 } @s += Integer(it.first); nil END{ [@s.to_s] }"

    cfg = make_config(
      type_name:     "pipe",
      in_endpoints:  [OMQ::CLI::Endpoint.new(work_url,    false)],
      out_endpoints: [OMQ::CLI::Endpoint.new(results_url, false)],
      parallel:      2,
      recv_expr:     expr,
      timeout:       PARALLEL_IDLE_TIMEOUT,
    )

    io_thread = Thread.new do
      Sync do
        src  = OMQ::PUSH.new(linger: 1)
        src.bind(work_url)
        sink = OMQ::PULL.new(recv_timeout: PARALLEL_TEST_TIMEOUT)
        sink.bind(results_url)

        src.peer_connected.wait
        (1..10).each { |n| src.send([n.to_s]) }

        2.times.map { sink.receive&.first.to_i }.sum
      ensure
        src&.close
        sink&.close
      end
    end

    runner_thread = run_pipe_runner(cfg)
    runner_thread.join
    total = io_thread.value
    assert_equal 55, total
  end
end
