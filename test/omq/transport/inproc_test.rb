# frozen_string_literal: true

require_relative "../../test_helper"

class OMQTransportInprocTest < Minitest::Spec
  describe OMQ::Transport::Inproc do
    Inproc = OMQ::Transport::Inproc

    before { Inproc.reset! }

    it "treats inproc:// as a ruby:// alias" do
      Async do
        endpoint = "inproc-alias-#{object_id}"
        server   = OMQ::PAIR.bind("inproc://#{endpoint}")
        client   = OMQ::PAIR.connect("inproc://#{endpoint}")

        assert_equal "ruby://#{endpoint}", server.engine.listeners.keys.fetch(0)

        client << "hello"
        assert_equal ["hello"], server.receive
      ensure
        client&.close
        server&.close
      end
    end


    it "shares one namespace between ruby:// and inproc://" do
      Async do
        endpoint = "inproc-ruby-alias-#{object_id}"
        server   = OMQ::PAIR.bind("ruby://#{endpoint}")
        client   = OMQ::PAIR.connect("inproc://#{endpoint}")

        client << "hello"
        assert_equal ["hello"], server.receive
      ensure
        client&.close
        server&.close
      end
    end


    it "lets a registered inproc transport override the alias" do
      original_transports = OMQ::Engine.transports
      fake_transport      = Module.new

      OMQ::Engine.instance_variable_set(:@transports, original_transports.dup)
      OMQ::Engine.transports["inproc"] = fake_transport

      engine = OMQ::Engine.new(:PAIR, OMQ::Options.new)

      assert_same fake_transport, engine.transport_for("inproc://native")
    ensure
      OMQ::Engine.instance_variable_set(:@transports, original_transports)
    end


    it "reports an incompatible binder and links once a compatible one binds" do
      Async do
        events = Async::Queue.new
        push   = OMQ::PUSH.new(linger: 0)
        push.reconnect_interval = RECONNECT_INTERVAL
        push.monitor { |event| events << event }
        push.connect("inproc://inproc-refused")

        bad = OMQ::PUB.bind("inproc://inproc-refused")

        refused = nil
        until refused
          event   = events.dequeue
          refused = event if event.type == :connect_retried && event.detail[:error].is_a?(Protocol::ZMTP::Error)
        end
        assert_match(/incompatible socket types/, refused.detail[:error].message)

        bad.close
        good = OMQ::PULL.bind("inproc://inproc-refused")
        push << "linked"
        assert_equal ["linked"], good.receive
      ensure
        push&.close
        bad&.close
        good&.close
      end
    end


    describe "Pipe" do
      it "transforms buffered messages and drops nil results before and after wiring" do
        Async do
          pipe  = Inproc::Pipe.new(peer_identity: "", peer_type: "DEALER")
          queue = Async::Queue.new

          pipe.send_message(["bad"])
          pipe.send_message(["", "buffered"])
          pipe.wire_direct_recv(queue, ->(parts) { parts[1..] if parts.first.empty? })
          pipe.send_message(["bad"])
          pipe.send_message(["", "live"])

          assert_equal ["buffered"], queue.dequeue
          assert_equal ["live"], queue.dequeue
          assert queue.empty?
        end.wait
      end


      it "transfers messages bidirectionally" do
        Async do
          a_to_b = Async::Queue.new
          b_to_a = Async::Queue.new
          side_a = Inproc::Pipe.new(
            send_queue:    a_to_b,
            receive_queue: b_to_a,
            peer_identity: "",
            peer_type:     "PAIR",
          )
          side_b = Inproc::Pipe.new(
            send_queue:    b_to_a,
            receive_queue: a_to_b,
            peer_identity: "",
            peer_type:     "PAIR",
          )

          Async do
            side_a.send_message(["hello from A"])
            received = side_b.receive_message
            assert_equal ["hello from A"], received

            side_b.send_message(["hello from B"])
            received = side_a.receive_message
            assert_equal ["hello from B"], received
          end.wait
        end
      end

      it "delivers message parts across the pipe" do
        Async do
          a_to_b = Async::Queue.new
          b_to_a = Async::Queue.new
          side_a = Inproc::Pipe.new(
            send_queue:    a_to_b,
            receive_queue: b_to_a,
            peer_identity: "",
            peer_type:     "PAIR",
          )
          side_b = Inproc::Pipe.new(
            send_queue:    b_to_a,
            receive_queue: a_to_b,
            peer_identity: "",
            peer_type:     "PAIR",
          )

          Async do
            side_a.send_message(["hello"])

            received = side_b.receive_message
            assert_equal ["hello"], received
          end.wait
        end
      end

      it "raises EOFError on receive after close" do
        Async do
          a_to_b = Async::Queue.new
          b_to_a = Async::Queue.new
          side_a = Inproc::Pipe.new(
            send_queue:    a_to_b,
            receive_queue: b_to_a,
            peer_identity: "",
            peer_type:     "PAIR",
          )
          side_b = Inproc::Pipe.new(
            send_queue:    b_to_a,
            receive_queue: a_to_b,
            peer_identity: "",
            peer_type:     "PAIR",
          )

          Async do
            side_a.close
            assert_raises(EOFError) { side_b.receive_message }
          end.wait
        end
      end

      it "raises IOError on send after close" do
        Async do
          a_to_b = Async::Queue.new
          side_a = Inproc::Pipe.new(
            send_queue:    a_to_b,
            receive_queue: Async::Queue.new,
            peer_identity: "",
            peer_type:     "PAIR",
          )

          Async do
            side_a.close
            assert_raises(IOError) { side_a.send_message(["data"]) }
          end.wait
        end
      end
    end
  end
end
