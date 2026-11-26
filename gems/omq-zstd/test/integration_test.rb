# frozen_string_literal: true

require_relative "test_helper"

class OMQZstdIntegrationTest < Minitest::Spec
  describe "zstd+tcp:// transport" do
    it "round-trips a large payload with no dict" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("zstd+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        payload = "a" * 4096
        push << [payload]

        assert_equal [payload], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "round-trips a small payload below threshold" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("zstd+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        push << ["hi"]
        assert_equal ["hi"], pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    it "auto-trains and ships dict to receiver" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("zstd+tcp://127.0.0.1:0")
        push.connect(uri.to_s, auto_dict: true)

        template = "user=%s|status=active|tier=gold|region=eu-west-%d|payload=" + ("x" * 600)
        sent = 200.times.map { |i| format(template, "user_#{i}@example.com", i % 4) }
        sent.each { |m| push << [m] }

        received = sent.size.times.map { pull.receive.first }
        assert_equal sent, received
      ensure
        push&.close
        pull&.close
      end
    end


    it "rejects a byte bomb exceeding max_message_size" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        pull.max_message_size = 4096
        uri = pull.bind("zstd+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        push << ["A" * 1_048_576]

        Async::Task.current.with_timeout(1) { pull.all_peers_gone.wait }
        assert_empty pull.engine.connections
      ensure
        push&.close
        pull&.close
      end
    end


    it "keeps accepting after a transient accept error" do
      Sync do
        pull   = OMQ::PULL.new
        events = []
        pull.monitor { |event| events << event.type }
        uri    = pull.bind("zstd+tcp://127.0.0.1:0")
        server = pull.instance_variable_get(:@engine).listeners.values.first.instance_variable_get(:@servers).first

        # The accept after the first client fails once with EMFILE.
        original = server.method(:accept)
        failed   = false
        server.define_singleton_method(:accept) do |*args|
          unless failed
            failed = true
            raise Errno::EMFILE
          end

          original.call(*args)
        end

        first = OMQ::PUSH.new(linger: 0)
        first.connect(uri.to_s)
        first << ["first"]
        assert_equal ["first"], pull.receive

        second = OMQ::PUSH.new(linger: 0)
        second.connect(uri.to_s)
        second << ["second"]
        assert_equal ["second"], pull.receive
        assert_includes events, :accept_failed
      ensure
        second&.close
        first&.close
        pull&.close
      end
    end


    it "round-trips a multipart message" do
      Sync do
        pull = OMQ::PULL.new
        push = OMQ::PUSH.new
        uri  = pull.bind("zstd+tcp://127.0.0.1:0")
        push.connect(uri.to_s)

        parts = ["header-data " * 100, "body-content " * 200, "trailer " * 50]
        push << parts

        received = pull.receive
        assert_equal parts, received
      ensure
        push&.close
        pull&.close
      end
    end
  end
end
