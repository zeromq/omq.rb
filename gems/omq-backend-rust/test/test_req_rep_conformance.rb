# frozen_string_literal: true

require_relative "test_helper"

class OMQBackendRustReqRepConformanceTest < Minitest::Spec
  before { skip_without_ruby_backend }

  def scenario
    @sockets = []
    Async do |task|
      task.with_timeout(3) { yield task }
    ensure
      @sockets.reverse_each(&:close)
    end.wait
  end

  def socket(type, backend: :rust)
    OMQ.const_get(type).new(backend: backend, linger: 0).tap { |s| @sockets << s }
  end

  def bind(socket)
    socket.bind("tcp://127.0.0.1:0").to_s
  end

  describe "REQ replies from Ruby ROUTER peers" do
    [["bad"], [""]].each do |invalid_reply|
      it "discards #{invalid_reply.inspect} without completing the request" do
        scenario do
          router = socket(:ROUTER, backend: :ruby)
          req    = socket(:REQ)
          req.connect(bind(router))
          req << "request"
          identity, = router.receive
          router << [identity, *invalid_reply]
          router << [identity, "", "good"]
          assert_equal ["good"], req.receive
        end
      end
    end

    it "discards a reply from a peer that did not receive the request" do
      scenario do
        first = socket(:ROUTER, backend: :ruby)
        other = socket(:ROUTER, backend: :ruby)
        req   = socket(:REQ)
        req.identity = "audit-client"
        req.connect(bind(first))
        req << "request"
        first.receive
        req.connect(bind(other))
        other.peer_connected.wait
        other << ["audit-client", "", "unsolicited"]
        sleep 0.02
        first << ["audit-client", "", "correct"]
        assert_equal ["correct"], req.receive
      end
    end

    it "requires application receive before sending another request" do
      scenario do
        rep = socket(:REP, backend: :ruby)
        req = socket(:REQ)
        req.connect(bind(rep))
        req << "first"
        rep.receive
        rep << "reply"
        sleep 0.02
        assert_raises(RuntimeError) { req.send("second") }
        assert_equal ["reply"], req.receive
        req << "second"
        assert_equal ["second"], rep.receive
      end
    end
  end

  describe "REP requests from Ruby peers" do
    [["no-delimiter"], [""]].each do |invalid_request|
      it "discards #{invalid_request.inspect} without admitting a request" do
        scenario do
          rep    = socket(:REP)
          dealer = socket(:DEALER, backend: :ruby)
          dealer.connect(bind(rep))
          dealer << invalid_request
          dealer << ["", "valid"]
          assert_equal ["valid"], rep.receive
        end
      end
    end

    it "requires receive before sending a reply" do
      scenario do
        rep = socket(:REP)
        bind(rep)
        assert_raises(RuntimeError) { rep.send("unsolicited") }
      end
    end

    it "discards a disconnected client's reply without sending it to the next client" do
      scenario do
        rep      = socket(:REP)
        endpoint = bind(rep)
        first    = socket(:REQ, backend: :ruby)
        first.connect(endpoint)
        first << "first-request"
        assert_equal ["first-request"], rep.receive

        second = socket(:REQ, backend: :ruby)
        second.connect(endpoint)
        second.peer_connected.wait
        second << "second-request"
        sleep 0.02
        first.close
        sleep 0.05

        rep << "private-first-reply"
        assert_equal ["second-request"], rep.receive
        rep << "second-reply"
        assert_equal ["second-reply"], second.receive
      end
    end

    it "preserves routing envelopes and empty body frames" do
      scenario do
        rep    = socket(:REP)
        dealer = socket(:DEALER, backend: :ruby)
        dealer.connect(bind(rep))
        envelope = ["hop-a", "\x01\x00\x00\x00".b, ""]
        dealer << [*envelope, "", "payload"]
        assert_equal ["", "payload"], rep.receive
        rep << ["", "response"]
        assert_equal [*envelope, "", "response"], dealer.receive
      end
    end

    it "keeps the active reply destination when another request is prefetched" do
      scenario do
        rep      = socket(:REP)
        endpoint = bind(rep)
        first    = socket(:REQ, backend: :ruby)
        first.connect(endpoint)
        first << "first-request"
        assert_equal ["first-request"], rep.receive

        second = socket(:REQ, backend: :ruby)
        second.connect(endpoint)
        second << "second-request"
        sleep 0.03
        rep << "first-reply"
        assert_equal ["first-reply"], first.receive
        assert_equal ["second-request"], rep.receive
        rep << "second-reply"
        assert_equal ["second-reply"], second.receive
      end
    end
  end
end
