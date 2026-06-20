# frozen_string_literal: true

require_relative "../test_helper"

describe "ROUTER/DEALER broker over inproc" do
  before { OMQ::Transport::Inproc.reset! }

  # Helper: sets up a ROUTER→DEALER broker with N workers, yields
  # a connected REQ client. All sockets are cleaned up after the block.
  #
  def with_broker(task, workers: 1, client_id: "client-1")
    frontend = OMQ::ROUTER.bind("ruby://broker-fe-#{client_id}")
    backend  = OMQ::DEALER.bind("ruby://broker-be-#{client_id}")

    task.async(transient: true) do
      loop { backend << frontend.receive }
    end

    task.async(transient: true) do
      loop { frontend << backend.receive }
    end

    workers.times do
      task.async(transient: true) do
        rep = OMQ::REP.connect("ruby://broker-be-#{client_id}")
        loop { rep << rep.receive }
      ensure
        rep&.close
      end
    end

    req = OMQ::REQ.new
    req.identity = client_id
    req.connect("ruby://broker-fe-#{client_id}")

    yield req
  ensure
    req&.close
    frontend&.close
    backend&.close
  end

  it "routes a request through a ROUTER→DEALER broker to a REP worker" do
    result = Async do |task|
      with_broker(task) do |req|
        Async::Task.current.with_timeout(2) do
          req << "hello"
          req.receive
        end
      end
    end.wait
    assert_equal ["hello"], result
  end

  it "handles multiple round-trips through the broker" do
    replies = Async do |task|
      with_broker(task, client_id: "client-2") do |req|
        Async::Task.current.with_timeout(2) do
          10.times.map do |i|
            req << "msg-#{i}"
            req.receive
          end
        end
      end
    end.wait
    10.times do |i|
      assert_equal ["msg-#{i}"], replies[i]
    end
  end

  it "routes to multiple workers via round-robin" do
    replies = Async do |task|
      with_broker(task, workers: 4, client_id: "client-3") do |req|
        Async::Task.current.with_timeout(2) do
          20.times.map do |i|
            req << "msg-#{i}"
            req.receive
          end
        end
      end
    end.wait
    20.times do |i|
      assert_equal ["msg-#{i}"], replies[i]
    end
  end
end
