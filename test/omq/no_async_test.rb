# frozen_string_literal: true

require_relative "../test_helper"
require "timeout"

describe "non-Async usage" do
  before { OMQ::Transport::Inproc.reset! }

  it "sends and receives without an Async block" do
    pull = OMQ::PULL.new
    port = pull.bind("tcp://127.0.0.1:0").port
    push = OMQ::PUSH.connect("tcp://127.0.0.1:#{port}")

    push << "hello"
    assert_equal ["hello"], pull.receive
  ensure
    push&.close
    pull&.close
  end


  it "unregisters linger when a socket is closed before shutdown" do
    # skip 'non-Async seems broken'
    a = OMQ::PUSH.new.tap { |s| s.linger = 0 }
    b = OMQ::PUSH.new.tap { |s| s.linger = 0 }
    a.bind("tcp://127.0.0.1:0")
    b.bind("tcp://127.0.0.1:0")

    lingers = OMQ::Reactor.lingers
    assert_equal 2, lingers[0]

    a.close
    assert_equal 1, lingers[0]

    b.close
    assert_equal 0, lingers.fetch(0, 0)
  ensure
    a&.close
    b&.close
  end


  it "starts a fresh background reactor after fork" do
    OMQ::Reactor.root_task

    reader, writer = IO.pipe
    pid = fork do
      reader.close
      OMQ::Reactor.run { writer.write("ok") }
      writer.close
      exit! 0
    rescue => error
      writer.write("#{error.class}: #{error.message}") rescue nil
      writer.close rescue nil
      exit! 1
    end

    writer.close
    result = Timeout.timeout(2) { reader.read }
    _, status = Process.wait2(pid)

    assert_predicate status, :success?
    assert_equal "ok", result
  rescue Timeout::Error
    Process.kill("KILL", pid) rescue nil
    Process.wait(pid) rescue nil
    flunk "child inherited a stale OMQ::Reactor"
  ensure
    reader&.close
    writer&.close
  end
end
