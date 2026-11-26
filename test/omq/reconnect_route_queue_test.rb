# frozen_string_literal: true

require_relative "../test_helper"
require "securerandom"
require "omq/client_server"
require "omq/peer"

# A dialed peer keeps its send queue and routing id across reconnects:
# sends made while it is down reach the redialed connection.
#
class OMQReconnectRouteQueueTest < Minitest::Spec
  def endpoint
    @endpoint ||= "ipc://@omq-route-queue-#{SecureRandom.hex(4)}"
  end


  def new_socket(klass, **opts)
    klass.new.tap do |s|
      s.linger             = 0
      s.reconnect_interval = RECONNECT_INTERVAL
      opts.each { |k, v| s.public_send(:"#{k}=", v) }
    end
  end


  describe "ROUTER connect side" do
    it "delivers sends made while the peer was down" do
      Async do
        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(endpoint)
        router = new_socket(OMQ::ROUTER)
        router.connect(endpoint)
        router.peer_connected.wait

        dealer.close
        router.all_peers_gone.wait
        router.send(["d1", "while down"])

        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(endpoint)
        assert_equal ["while down"], dealer.receive
      ensure
        router&.close
        dealer&.close
      end
    end


    it "keeps a generated routing id" do
      Async do
        dealer = new_socket(OMQ::DEALER)
        dealer.bind(endpoint)
        router = new_socket(OMQ::ROUTER)
        router.connect(endpoint)

        dealer << "hi"
        id, _ = router.receive

        dealer.close
        router.all_peers_gone.wait
        router.send([id, "while down"])

        dealer = new_socket(OMQ::DEALER)
        dealer.bind(endpoint)
        assert_equal ["while down"], dealer.receive

        dealer << "back"
        assert_equal [id, "back"], router.receive
      ensure
        router&.close
        dealer&.close
      end
    end


    it "keeps the route over inproc" do
      Async do
        ep     = "ruby://route-queue-#{SecureRandom.hex(4)}"
        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(ep)
        router = new_socket(OMQ::ROUTER)
        router.connect(ep)
        router.peer_connected.wait

        dealer.close
        router.all_peers_gone.wait
        router.send(["d1", "while down"])

        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(ep)
        assert_equal ["while down"], dealer.receive
      ensure
        router&.close
        dealer&.close
      end
    end


    it "routes to the id until #disconnect drops it" do
      Async do
        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(endpoint)
        router = new_socket(OMQ::ROUTER, router_mandatory: true)
        router.connect(endpoint)
        router.peer_connected.wait

        dealer.close
        router.all_peers_gone.wait
        router.send(["d1", "queued"])

        router.disconnect(endpoint)
        assert_raises(SocketError) { router.send(["d1", "gone"]) }
      ensure
        router&.close
        dealer&.close
      end
    end


    it "redials during linger to deliver kept sends" do
      Async do |task|
        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(endpoint)
        router = new_socket(OMQ::ROUTER, linger: 0.5)
        router.connect(endpoint)
        router.peer_connected.wait

        dealer.close
        router.all_peers_gone.wait
        router.send(["d1", "before close"])
        closer = task.async { router.close }

        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.bind(endpoint)
        assert_equal ["before close"], dealer.receive
        closer.wait
      ensure
        router&.close
        dealer&.close
      end
    end  end


  describe "ROUTER bind side" do
    it "forgets a peer once it disconnects" do
      Async do
        router = new_socket(OMQ::ROUTER, router_mandatory: true)
        router.bind(endpoint)
        dealer = new_socket(OMQ::DEALER, identity: "d1")
        dealer.connect(endpoint)
        router.peer_connected.wait

        dealer.close
        router.all_peers_gone.wait
        assert_raises(SocketError) { router.send(["d1", "x"]) }
      ensure
        dealer&.close
        router&.close
      end
    end
  end


  describe "SERVER connect side" do
    it "delivers sends made while the peer was down" do
      Async do
        client = new_socket(OMQ::CLIENT)
        client.bind(endpoint)
        server = new_socket(OMQ::SERVER)
        server.connect(endpoint)

        client << "hi"
        id, _ = server.receive

        client.close
        server.all_peers_gone.wait
        server.send_to(id, "while down")

        client = new_socket(OMQ::CLIENT)
        client.bind(endpoint)
        assert_equal ["while down"], client.receive
      ensure
        server&.close
        client&.close
      end
    end
  end


  describe "PEER connect side" do
    it "delivers sends made while the peer was down" do
      Async do
        a = new_socket(OMQ::PEER)
        a.bind(endpoint)
        b = new_socket(OMQ::PEER)
        b.connect(endpoint)
        wait_connected(a, b)

        a.send_to(a.engine.routing.connections_by_routing_id.keys.first, "hi")
        a_id, _ = b.receive

        a.close
        b.all_peers_gone.wait
        b.send_to(a_id, "while down")

        a = new_socket(OMQ::PEER)
        a.bind(endpoint)
        assert_equal "while down", a.receive.last
      ensure
        b&.close
        a&.close
      end
    end
  end
end
