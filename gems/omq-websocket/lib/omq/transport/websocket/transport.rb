# frozen_string_literal: true

require "uri"
require "async"
require "async/promise"
require "async/http/endpoint"
require "async/http/server"
require "async/http/protocol/http1"
require "async/websocket/client"
require "async/websocket/adapters/http"
require "protocol/http/response"

module OMQ
  module Transport
    # ZeroMQ-over-WebSocket transport (ZWS 2.0, RFC 45). Registered
    # for both +ws://+ and +wss://+ schemes. The only difference
    # between the two is whether the underlying HTTP endpoint carries
    # an SSL context (supplied via +socket.tls_context=+).
    module WebSocket

      class << self

        # Engine reads this in ConnectionLifecycle#handshake! to
        # construct the per-connection codec/handshake driver.
        # ZWS framing differs from ZMTP/3.1, so we substitute our own
        # Connection class instead of Protocol::ZMTP::Connection.
        #
        # @return [Class]
        #
        def connection_class
          OMQ::Transport::WebSocket::Connection
        end


        # Creates a bound WebSocket listener.
        #
        # @param endpoint [String] e.g. "ws://127.0.0.1:5555/" or "wss://host:443/zmq"
        # @param engine [Engine]
        # @return [Listener]
        # @raise [Error] if wss:// is used without a tls_context
        #
        def listener(endpoint, engine, **)
          scheme = endpoint[/\A([a-z]+):\/\//, 1]
          tls    = engine.options.tls_context

          if scheme == "wss" && tls.nil?
            raise Error, "wss:// bind requires options.tls_context"
          end

          subprotocols  = engine.options.ws_subprotocols
          path          = engine.options.ws_path
          http_endpoint = parse_http_endpoint(endpoint, tls)
          bound         = http_endpoint.bound
          actual_port   = bound.sockets.first.to_io.local_address.ip_port
          host          = http_endpoint.hostname
          host_part     = host.include?(":") ? "[#{host}]" : host
          shown         = "#{scheme}://#{host_part}:#{actual_port}#{path}"

          Listener.new(
            shown_endpoint: shown,
            bound:          bound,
            http_endpoint:  http_endpoint,
            subprotocols:   subprotocols,
            match_path:     path,
            engine:         engine,
            port:           actual_port,
          )
        end


        # Creates a WebSocket dialer.
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [Dialer]
        #
        def dialer(endpoint, engine, **)
          Dialer.new(endpoint, engine)
        end


        # Verifies that the endpoint URI is well-formed for HTTP/WS.
        #
        # @param endpoint [String]
        # @raise [ArgumentError]
        #
        def validate_endpoint!(endpoint)
          uri = URI.parse(endpoint)
          raise ArgumentError, "missing host: #{endpoint.inspect}" unless uri.hostname
          raise ArgumentError, "missing port: #{endpoint.inspect}" unless uri.port
        end


        # Parses +endpoint+ into an Async::HTTP::Endpoint, attaching the
        # SSL context when present (TLS for wss://).
        #
        # @param endpoint [String]
        # @param tls_context [OpenSSL::SSL::SSLContext, nil]
        # @return [Async::HTTP::Endpoint]
        #
        def parse_http_endpoint(endpoint, tls_context)
          if tls_context
            ::Async::HTTP::Endpoint.parse(endpoint, ssl_context: tls_context)
          else
            ::Async::HTTP::Endpoint.parse(endpoint)
          end
        end

      end


      # Outgoing connection factory. Stateful so reconnect can call
      # +#connect+ again without redoing endpoint parsing.
      class Dialer

        attr_reader :endpoint


        def initialize(endpoint, engine)
          @endpoint = endpoint
          @engine   = engine
        end


        # Establishes the WebSocket upgrade and hands the resulting
        # Async::WebSocket::Connection to the engine. The engine builds
        # the OMQ::Transport::WebSocket::Connection on top via the
        # transport's +connection_class+ hook and runs the ZWS
        # handshake.
        #
        # @return [void]
        #
        def connect
          tls           = @engine.options.tls_context
          subprotocols  = @engine.options.ws_subprotocols
          http_endpoint = WebSocket.parse_http_endpoint(@endpoint, tls)
          ws_conn       = ::Async::WebSocket::Client.connect(
            http_endpoint,
            protocols: subprotocols,
          )

          @engine.handle_connected(ws_conn, endpoint: @endpoint)
        end

      end


      # Bound WebSocket listener. Wraps an Async::HTTP::Server bound on
      # the requested host/port. Each accepted HTTP request is matched
      # against +ws_path+ and upgraded via
      # Async::WebSocket::Adapters::HTTP.open. The adapter block stays
      # alive (via AcceptedConnection#wait_for_close) until the engine
      # closes the connection — otherwise Adapters::HTTP.open would
      # tear the WebSocket down the moment the block exits.
      class Listener

        attr_reader :endpoint
        attr_reader :port


        def initialize(shown_endpoint:, bound:, http_endpoint:, subprotocols:, match_path:, engine:, port:)
          @endpoint      = shown_endpoint
          @bound         = bound
          @http_endpoint = http_endpoint
          @subprotocols  = subprotocols
          @match_path    = match_path
          @engine        = engine
          @port          = port
          @task          = nil
        end


        def start_accept_loops(parent_task, &on_accepted)
          @task = parent_task.async(transient: true, annotation: "ws accept #{@endpoint}") do |task|
            server = ::Async::HTTP::Server.new(
              ->(request) { handle_request(request, on_accepted) },
              @bound,
              protocol: ::Async::HTTP::Protocol::HTTP1,
              scheme:   @http_endpoint.secure? ? "https" : "http",
            )

            @bound.accept(&server.method(:accept))

            # +accept+ fires per-socket accept fibers and returns. Wait
            # on them so the +ensure+ doesn't close +@bound+ out from
            # under a live accept.
            task.children.each(&:wait)
          rescue ::Async::Stop
            # socket barrier stopped — clean cancel
          ensure
            @bound.close rescue nil
          end
        end


        def stop
          @task&.stop
          @bound.close rescue nil
        end


        private


        def handle_request(request, on_accepted)
          return not_found if @match_path && request.path != @match_path

          ::Async::WebSocket::Adapters::HTTP.open(request, protocols: @subprotocols) do |ws_conn|
            accepted = AcceptedConnection.new(ws_conn)
            on_accepted.call(accepted)
            accepted.wait_for_close
          end or not_found
        end


        def not_found
          ::Protocol::HTTP::Response[404, {}, []]
        end

      end


      # Server-side wrapper around an Async::WebSocket::Connection.
      # Delegates the WS interface used by Connection (+read+,
      # +send_binary+, +flush+, +protocol+) and adds +wait_for_close+
      # so the Adapters::HTTP.open fiber can block until the engine
      # closes the connection.
      #
      # +close+ only resolves the promise — the actual WebSocket close
      # is performed by Adapters::HTTP.open's ensure block once
      # +wait_for_close+ returns. Calling +@ws.close+ here too would
      # double-close and (on Ruby 3.3) deadlock when the peer has
      # already closed.
      class AcceptedConnection

        def initialize(ws)
          @ws             = ws
          @closed_promise = ::Async::Promise.new
        end


        def protocol
          @ws.protocol
        end


        def read
          @ws.read
        end


        def send_binary(buffer, **opts)
          @ws.send_binary(buffer, **opts)
        end


        def flush
          @ws.flush
        end


        def close
          @closed_promise.resolve(true)
        end


        def closed?
          @closed_promise.resolved?
        end


        def wait_for_close
          @closed_promise.wait
        end

      end

    end
  end
end
