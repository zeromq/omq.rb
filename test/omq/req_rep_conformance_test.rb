# frozen_string_literal: true

require_relative "../test_helper"

class OMQReqRepConformanceTest < Minitest::Spec
  def scenario
    @sockets = []
    Async do |task|
      yield task
    ensure
      @sockets.reverse_each(&:close)
    end.wait
  end

  def socket(type, **options)
    OMQ.const_get(type).new(backend: :ruby, linger: 0, **options).tap { |s| @sockets << s }
  end

  def bind(s)
    if @transport == :inproc
      endpoint = "ruby://conformance-#{object_id}-#{s.object_id}"
      s.bind(endpoint)
      endpoint
    else
      "tcp://127.0.0.1:#{s.bind('tcp://127.0.0.1:0').port}"
    end
  end

  def connected(*sockets)
    sockets.each { |s| s.peer_connected.wait }
  end

  [:tcp, :inproc].each do |transport|
    describe "REQ/REP over #{transport}" do
      before { @transport = transport }

      it "REQ requires a send before receiving" do
        scenario do
          req = socket(:REQ)
          assert_raises(SocketError) { req.receive }
        end
      end

      it "enforces alternation across complete request/reply cycles" do
        scenario do
          rep = socket(:REP)
          req = socket(:REQ)
          req.connect(bind(rep))

          2.times do
            req << "request"
            assert_raises(SocketError) { req.send("second") }
            assert_equal ["request"], rep.receive
            assert_raises(SocketError) { rep.receive }
            rep << "reply"
            assert_raises(SocketError) { rep.send("second") }
            assert_equal ["reply"], req.receive
            assert_raises(SocketError) { req.receive }
          end
        end
      end

      it "REQ discards duplicate replies already queued by the same peer" do
        scenario do
          router = socket(:ROUTER)
          req    = socket(:REQ)
          req.connect(bind(router))
          req << "request"
          identity, = router.receive
          router << [identity, "", "reply"]
          router << [identity, "", "duplicate"]
          sleep 0.02
          assert_equal ["reply"], req.receive

          req << "next request"
          router.receive
          router << [identity, "", "next reply"]
          assert_equal ["next reply"], req.receive
        end
      end

      it "REQ can resume receiving after a timeout" do
        scenario do |task|
          rep = socket(:REP)
          req = socket(:REQ)
          req.connect(bind(rep))
          req << "request"
          rep.receive
          assert_raises(Async::TimeoutError) { task.with_timeout(0.01) { req.receive } }
          assert_raises(SocketError) { req.send("second") }
          rep << "reply"
          assert_equal ["reply"], req.receive
        end
      end

      if transport == :inproc
        it "REP discards replies when a peer's send queue is full" do
          scenario do |task|
            rep    = socket(:REP, send_hwm: 1)
            dealer = socket(:DEALER, recv_hwm: 1)
            dealer.connect(bind(rep))
            10.times { dealer << ["", "request"] }

            10.times do
              assert_equal ["request"], rep.receive
              rep << "reply"
              task.yield
            end
          end
        end
      end

      it "REQ rejects missing delimiter" do
        scenario do
          router = socket(:ROUTER)
          req    = socket(:REQ)
          req.connect(bind(router))
          req << "request"
          identity, = router.receive
          router << [identity, "bad"]
          router << [identity, "", "good"]
          assert_equal ["good"], req.receive
        end
      end

      it "REQ rejects delimiter without body" do
        scenario do
          router = socket(:ROUTER)
          req    = socket(:REQ)
          req.connect(bind(router))
          req << "request"
          identity, = router.receive
          router << [identity, ""]
          router << [identity, "", "good"]
          assert_equal ["good"], req.receive
        end
      end

      it "REQ discards reply from another peer" do
        scenario do
          first = socket(:ROUTER)
          other = socket(:ROUTER)
          req   = socket(:REQ)
          req.identity = "audit-client"
          req.connect(bind(first))
          req << "request"
          first.receive
          req.connect(bind(other))
          connected(other)
          other << ["audit-client", "", "unsolicited"]
          sleep 0.02
          first << ["audit-client", "", "correct"]
          assert_equal ["correct"], req.receive
        end
      end

      it "REQ cannot send again until application receives" do
        scenario do
          rep = socket(:REP)
          req = socket(:REQ)
          req.connect(bind(rep))
          req << "first"
          rep.receive
          rep << "reply"
          sleep 0.02
          assert_raises(SocketError) { req.send("second") }
        end
      end

      it "REP rejects missing delimiter" do
        scenario do
          rep    = socket(:REP)
          dealer = socket(:DEALER)
          dealer.connect(bind(rep))
          dealer << ["no-delimiter"]
          dealer << ["", "valid"]
          assert_equal ["valid"], rep.receive
        end
      end

      it "REP rejects delimiter without body" do
        scenario do
          rep    = socket(:REP)
          dealer = socket(:DEALER)
          dealer.connect(bind(rep))
          dealer << [""]
          dealer << ["", "valid"]
          assert_equal ["valid"], rep.receive
        end
      end

      it "REP requires receive before send" do
        scenario do
          rep = socket(:REP)
          bind(rep)
          assert_raises(SocketError) { rep.send("unsolicited") }
        end
      end

      if transport == :tcp
        it "REP does not send dead clients reply to next client" do
          scenario do
            rep      = socket(:REP)
            endpoint = bind(rep)
            first    = socket(:REQ)
            first.connect(endpoint)
            first << "first-request"
            assert_equal ["first-request"], rep.receive

            second = socket(:REQ)
            second.connect(endpoint)
            connected(second)
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
      end

      it "REP preserves envelope and empty body frames" do
        scenario do
          rep    = socket(:REP)
          dealer = socket(:DEALER)
          dealer.connect(bind(rep))
          envelope = ["hop-a", "\x01\x00\x00\x00".b, ""]
          dealer << [*envelope, "", "payload"]
          assert_equal ["", "payload"], rep.receive
          rep << ["", "response"]
          assert_equal [*envelope, "", "response"], dealer.receive
        end
      end

      it "REP keeps active peer when another request arrives" do
        scenario do |task|
          rep      = socket(:REP)
          endpoint = bind(rep)
          first    = socket(:REQ)
          first.connect(endpoint)
          first << "first-request"
          assert_equal ["first-request"], rep.receive
          second = socket(:REQ)
          second.connect(endpoint)
          second << "second-request"
          sleep 0.03
          rep << "first-reply"
          assert_equal ["first-reply"], task.with_timeout(0.2) { first.receive }
          assert_equal ["second-request"], rep.receive
          rep << "second-reply"
          assert_equal ["second-reply"], second.receive
        end
      end
    end
  end
end
