# frozen_string_literal: true

require_relative "../test_helper"
require "omq/scatter_gather"

# Disconnect-then-reconnect must work for SCATTER/GATHER.
# The connecting side detects peer loss and auto-reconnects.
#
class OMQReconnectScatterGatherTest < Minitest::Spec
  describe "Reconnect after server restart — SCATTER/GATHER" do

    # Helper: connect side reconnects after bind side restarts on same port.
    #
    def assert_reconnects(bind_class, connect_class, &exchange)
      Sync do
        server = bind_class.new(nil, linger: 0)
        port = server.bind("tcp://127.0.0.1:0").port

        client = connect_class.new(nil, linger: 0)
        client.reconnect_interval = RECONNECT_INTERVAL
        client.connect("tcp://127.0.0.1:#{port}")
        wait_connected(client, server)

        exchange.call(client, server)

        server.close
        sleep 0.02

        server2 = bind_class.new(nil, linger: 0)
        server2.bind("tcp://127.0.0.1:#{port}")
        wait_connected(client, server2)

        exchange.call(client, server2)
      ensure
        client&.close
        server2&.close
      end
    end

    it "SCATTER/GATHER" do
      assert_reconnects(OMQ::GATHER, OMQ::SCATTER) do |scatter, gather|
        scatter.send("task")
        assert_equal ["task"], gather.receive
      end
    end
  end
end
