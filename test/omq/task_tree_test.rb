# frozen_string_literal: true

require_relative "../test_helper"

class OMQTaskTreeTest < Minitest::Spec
  describe "Per-connection task tree" do
    # Per-connection pumps (recv pump, heartbeat, reaper, send pump) now
    # live on the *socket-level* Async::Barrier (SocketLifecycle#barrier).
    # Each connection also owns a nested per-connection Async::Barrier so
    # a failing pump can cascade-cancel its siblings. Both barriers share
    # the same parent task tree, so every pump is reachable from the
    # socket's root task and +barrier.stop+ cascades through everything
    # in one call.

    def capture_hierarchy(task)
      lines = []
      task.traverse do |node, level|
        ann = node.respond_to?(:annotation) ? node.annotation : node.class.name
        lines << [level, ann.to_s]
      end
      lines
    end


    it "recv pump and heartbeat are children of the connection task" do
      Async do |task|
        pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
        pull.heartbeat_interval = 1
        port = pull.bind("tcp://127.0.0.1:0").port

        push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
        push.heartbeat_interval = 1
        push.connect("tcp://127.0.0.1:#{port}")
        wait_connected(push)

        push << "hello"
        assert_equal ["hello"], pull.receive

        hierarchy = capture_hierarchy(task)
        annotations = hierarchy.map(&:last)

        assert_includes annotations.grep(/\Aconn /).any? ? annotations : [], annotations.grep(/\Aconn /).first,
                        "should have connection tasks"
        %w[recv\ pump heartbeat].each do |name|
          refute_empty annotations.select { |ann| ann == name }, "should have #{name} task reachable from socket root"
        end
      ensure
        push&.close
        pull&.close
      end
    end


    it "reaper is reachable from the socket root task on PUSH" do
      Async do |task|
        push = OMQ::PUSH.new.tap { |s| s.linger = 0 }
        port = push.bind("tcp://127.0.0.1:0").port

        pull = OMQ::PULL.new.tap { |s| s.linger = 0 }
        pull.connect("tcp://127.0.0.1:#{port}")
        wait_connected(pull)

        push << "hello"
        assert_equal ["hello"], pull.receive

        annotations = capture_hierarchy(task).map(&:last)
        refute_empty annotations.select { |ann| ann == "reaper" }, "PUSH should have a reaper task reachable from socket root"
      ensure
        pull&.close
        push&.close
      end
    end
  end
end
