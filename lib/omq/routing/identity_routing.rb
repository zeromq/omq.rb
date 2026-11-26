# frozen_string_literal: true

module OMQ
  module Routing
    # Send queues and routing ids for identity-routed sockets (ROUTER,
    # SERVER, PEER).
    #
    # Each peer has a route: its routing id and a bounded send queue that
    # its own pump drains. A dialed endpoint keeps its route across
    # reconnects, like a libzmq pipe: sends to that id while the peer is
    # down wait in the queue, and the redialed connection delivers them.
    # A message the lost connection's pump had already taken is gone.
    # Accepted peers lose their route with the connection, since a peer
    # that comes back can't be told apart from a new one.
    #
    # Including classes call {#init_identity_routing} from #initialize
    # and implement +#generate_routing_id+.
    #
    module IdentityRouting
      # One peer's routing id and send queue. +endpoint+ is set for a
      # dialed peer, +conn+ while it is connected.
      Route = Struct.new(:id, :queue, :endpoint, :conn)


      # @return [Boolean] true when every send queue is empty, including
      #   those of dialed peers that are down
      #
      def send_queues_drained?
        @route_by_conn.each_value.all? { |route| route.queue.empty? } &&
          @dial_routes.each_value.all? { |route| route.queue.empty? }
      end


      # Closes every queue, including those of dialed peers that are down.
      #
      # @return [void]
      #
      def close_queues
        super
        all_routes.each { |route| route.queue.close }
      end


      # Drops the route kept for a dialed endpoint. Called by
      # {Engine#disconnect}, and when a redial won't happen.
      #
      # @param endpoint [String]
      # @return [void]
      #
      def drop_route(endpoint)
        route = @dial_routes[endpoint]
        drop(route) if route && route.conn.nil?
      end


      private


      def init_identity_routing(engine)
        @routes        = {} # routing id => Route
        @route_by_conn = {} # connection => Route
        @dial_routes   = {} # endpoint => Route kept across redials
      end


      # Registers +conn+ and starts its send pump. A dialed connection
      # takes over its endpoint's route, so queued sends go out.
      #
      # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
      # @param peer_id [String, nil] id the peer asked for (ROUTER)
      # @return [Route]
      #
      def attach_route(conn, peer_id = nil)
        endpoint = @engine.dialed_endpoint(conn)
        route    = endpoint && @dial_routes[endpoint]
        route    = nil if route&.conn
        route  ||= Route.new(nil, Routing.build_queue(@engine.options.send_hwm, :block), endpoint)
        id       = peer_id || route.id || generate_routing_id

        unroute(route) if route.id && route.id != id

        route.id   = id
        route.conn = conn
        @routes[id]            = route # a newer peer takes over the id
        @route_by_conn[conn]   = route
        @dial_routes[endpoint] = route if endpoint

        ConnSendPump.start(@engine, conn, route.queue)
        route
      end


      # Unregisters +conn+. Its route stays if the endpoint will be
      # redialed.
      #
      # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
      # @return [void]
      #
      def detach_route(conn)
        route = @route_by_conn.delete(conn) or return
        route.conn = nil

        endpoint = route.endpoint
        return if endpoint && @dial_routes[endpoint].equal?(route) && @engine.redials?(endpoint)

        drop(route)
      end


      # @param id [String]
      # @return [Route, nil]
      #
      def route_to(id)
        @routes[id]
      end


      def drop(route)
        unroute(route)
        @dial_routes.delete(route.endpoint) if @dial_routes[route.endpoint].equal?(route)
        route.queue.close
      end


      def unroute(route)
        @routes.delete(route.id) if @routes[route.id].equal?(route)
      end


      def all_routes
        (@route_by_conn.values + @dial_routes.values).uniq
      end

    end
  end
end
