# frozen_string_literal: true

require_relative "../../test_helper"
require "timeout"

TRACE_ORDER_TEST_TIMEOUT = ENV.fetch("OMQ_TEST_TIMEOUT", "30").to_f


def join_or_raise(thread, message)
  thread.join(TRACE_ORDER_TEST_TIMEOUT) or raise message
end

# -- -vvv trace ordering ---------------------------------------------
#
# The `<<` receive trace must land *before* any stdout side-effects
# from the recv-expression eval (e.g. `-e 'p it'`) and before the
# formatted body. The previous design emitted the trace from the
# monitor fiber, which raced with the app fiber on a shared tty;
# the current design emits it from trace_recv(), called before
# eval_recv_expr() runs, so the interleaving is strict.

describe "trace_recv ordering at -vvv" do
  it "writes the << line before the body" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", verbose: 3),
      OMQ::PULL
    )
    err = StringIO.new
    out = StringIO.new
    $stderr = err
    $stdout = out
    runner.send(:trace_recv, ["hello"])
    runner.send(:output, ["hello"])
    $stderr = STDERR
    $stdout = STDOUT
    assert_match(/omq: << \(5B\) hello/, err.string)
    assert_equal "hello\n", out.string
  end

  it "is a no-op below -vvv" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", verbose: 2),
      OMQ::PULL
    )
    err = StringIO.new
    $stderr = err
    runner.send(:trace_recv, ["hello"])
    $stderr = STDERR
    assert_equal "", err.string
  end

  it "sanitizes binary/newline content in the preview" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", verbose: 3),
      OMQ::PULL
    )
    err = StringIO.new
    $stderr = err
    runner.send(:trace_recv, ["hi\nthere\tnow"])
    $stderr = STDERR
    line = err.string.chomp
    refute_includes line, "\n"
    refute_includes line, "\t"
    assert_match(/hi\\nthere\\tnow/, line)
  end

  it "marshal payload traces show app-level object, not wire bytes (end-to-end)" do
    url = ipc_url("pull-marshal-trace")

    pull_cfg = make_config(
      type_name: "pull",
      connects:  [url],
      verbose:   3,
      format:    :marshal,
      timeout:   TRACE_ORDER_TEST_TIMEOUT,
      count:     1,
    )

    combined = StringIO.new
    orig_stdout, orig_stderr = $stdout, $stderr
    $stdout = combined
    $stderr = combined

    io_thread = Thread.new do
      Sync do
        src = OMQ::PUSH.new(linger: 1)
        src.bind(url)
        src.peer_connected.wait
        # Raw-object marshal: one Ruby object per wire frame.
        src.send([Marshal.dump([nil, :foo, "bar"])])
      ensure
        src&.close
      end
    end

    runner_thread = Thread.new do
      Sync do |task|
        OMQ::CLI::PullRunner.new(pull_cfg, OMQ::PULL).call(task)
      end
    end

    join_or_raise(runner_thread, "PullRunner hung")
    io_thread.join
    $stdout = orig_stdout
    $stderr = orig_stderr

    output = combined.string
    assert_match(/omq: << \(\d+B marshal\) \[nil, :foo, "bar"\]/, output,
      "expected marshal-aware trace line in:\n#{output}")
    # Body is printed via Formatter#encode(:marshal) which inspects
    # the raw object directly (no array-of-frames wrapping).
    assert_includes output, %q{[nil, :foo, "bar"]},
      "expected body inspect in:\n#{output}"
  end

  it "marshal eval can transform strings into arbitrary objects (end-to-end)" do
    # Mirrors: omq push -ME '"foo"' | omq pull -Mvvv -e '{it => it.encoding}'
    # Expected body: {"foo" => #<Encoding:UTF-8>}
    url = ipc_url("pull-marshal-eval")

    pull_cfg = make_config(
      type_name: "pull",
      connects:  [url],
      verbose:   3,
      format:    :marshal,
      recv_expr: "{it => it.encoding}",
      timeout:   TRACE_ORDER_TEST_TIMEOUT,
      count:     1,
    )

    combined = StringIO.new
    orig_stdout, orig_stderr = $stdout, $stderr
    $stdout = combined
    $stderr = combined

    io_thread = Thread.new do
      Sync do
        src = OMQ::PUSH.new(linger: 1)
        src.bind(url)
        src.peer_connected.wait
        src.send([Marshal.dump("foo")])
      ensure
        src&.close
      end
    end

    runner_thread = Thread.new do
      Sync do |task|
        OMQ::CLI::PullRunner.new(pull_cfg, OMQ::PULL).call(task)
      end
    end

    join_or_raise(runner_thread, "PullRunner hung")
    io_thread.join
    $stdout = orig_stdout
    $stderr = orig_stderr

    output = combined.string
    assert_match(/omq: << \(\d+B marshal\) "foo"/, output,
      "expected << trace of raw string payload in:\n#{output}")
    assert_includes output, '{"foo" => #<Encoding:UTF-8>}',
      "expected eval-transformed hash body in:\n#{output}"
  end

  it "marshal send path: PushRunner sends raw object (end-to-end)" do
    # Mirrors: omq push -ME '"foo"' | omq pull -M
    url = ipc_url("push-marshal-send")

    push_cfg = make_config(
      type_name: "push",
      connects:  [url],
      format:    :marshal,
      send_expr: %q("foo"),
      count:     1,
      timeout:   TRACE_ORDER_TEST_TIMEOUT,
    )

    received = nil
    sink_ready = Thread::Queue.new
    io_thread = Thread.new do
      Sync do
        sink = OMQ::PULL.new
        sink.bind(url)
        sink_ready << true
        frames = sink.receive
        received = Marshal.load(frames.first)
      ensure
        sink&.close
      end
    end
    Timeout.timeout(TRACE_ORDER_TEST_TIMEOUT) { sink_ready.pop }

    runner_thread = Thread.new do
      Sync do |task|
        OMQ::CLI::PushRunner.new(push_cfg, OMQ::PUSH).call(task)
      end
    end

    join_or_raise(runner_thread, "PushRunner hung")
    join_or_raise(io_thread, "sink hung")

    assert_equal "foo", received
    assert_equal Encoding::UTF_8, received.encoding
  end

  it "trace_recv precedes eval stdout side-effects (end-to-end)" do
    url    = ipc_url("pull-trace-order")
    n_msgs = 3

    cfg = make_config(
      type_name: "pull",
      connects:  [url],
      verbose:   3,
      recv_expr: "p it.first",
      timeout:   TRACE_ORDER_TEST_TIMEOUT,
      count:     n_msgs,
    )

    # TEE stderr into stdout so the assertion can see the two streams
    # in the actual byte order they were written (can't compare
    # independent StringIO timestamps).
    combined = StringIO.new
    orig_stdout, orig_stderr = $stdout, $stderr
    $stdout = combined
    $stderr = combined

    io_thread = Thread.new do
      Sync do
        src = OMQ::PUSH.new(linger: 1)
        src.bind(url)
        src.peer_connected.wait
        n_msgs.times { |i| src.send(["msg-#{i}"]) }
      ensure
        src&.close
      end
    end

    runner_thread = Thread.new do
      Sync do |task|
        OMQ::CLI::PullRunner.new(cfg, OMQ::PULL).call(task)
      end
    end

    join_or_raise(runner_thread, "PullRunner hung")
    io_thread.join
    $stdout = orig_stdout
    $stderr = orig_stderr

    # Strict sequential match: for each message, the trace line, the
    # eval stdout ("p it.first" prints "msg-N"), and the body line
    # (^msg-N$) must appear in that order. Anchor body with ^ so it
    # doesn't match the msg-N substring inside the trace line.
    output = combined.string
    expected = n_msgs.times.flat_map do |i|
      msg = "msg-#{i}"
      [
        /^.*omq: << \(5B\) #{msg}$/,
        /^"#{msg}"$/,
        /^#{msg}$/,
      ]
    end
    pos = 0
    expected.each do |re|
      m = re.match(output, pos) or flunk "missing #{re.source} from offset #{pos} in:\n#{output}"
      pos = m.end(0)
    end
  end
end
