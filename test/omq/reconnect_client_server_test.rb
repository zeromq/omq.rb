# frozen_string_literal: true

require_relative "../test_helper"
require "omq/client_server"

# Disconnect-then-reconnect must work for CLIENT/SERVER.
# The connecting side detects peer loss and auto-reconnects.
#
class OMQReconnectClientServerTest < Minitest::Spec
  describe "Reconnect after server restart — CLIENT/SERVER" do
    it "CLIENT/SERVER" do
      Sync do
        server = OMQ::SERVER.new(nil, linger: 0)
        port = server.bind("tcp://127.0.0.1:0").port

        client = OMQ::CLIENT.new(nil, linger: 0)
        client.reconnect_interval = RECONNECT_INTERVAL
        client.connect("tcp://127.0.0.1:#{port}")
        wait_connected(client, server)

        client.send("req1")
        msg = server.receive
        assert_equal "req1", msg[1]
        server.send_to(msg[0], "rep1")
        assert_equal ["rep1"], client.receive

        server.close
        sleep 0.02

        server2 = OMQ::SERVER.new(nil, linger: 0)
        server2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(client, server2)

        client.send("req2")
        msg = server2.receive
        assert_equal "req2", msg[1]
      ensure
        client&.close
        server2&.close
      end
    end
  end
end
