# frozen_string_literal: true

require "socket"
require "uri"
require "io/stream"

module OMQ
  module Transport
    # TCP transport using Ruby sockets with Async.
    #
    module TCP
      Engine.transports["tcp"] = self


      class << self
        # ZMTP connection class used for TCP-accepted/dialed peers.
        #
        # @return [Class]
        #
        def connection_class
          Protocol::ZMTP::Connection
        end


        # Creates a bound TCP listener.
        #
        # @param endpoint [String] e.g. "tcp://127.0.0.1:5555" or "tcp://*:0"
        # @param engine [Engine]
        # @return [Listener]
        #
        def listener(endpoint, engine, **)
          host, port  = self.parse_endpoint(endpoint)
          lookup_host = normalize_bind_host(host)

          # Socket.tcp_server_sockets coordinates ephemeral ports across
          # address families and sets IPV6_V6ONLY so IPv4 and IPv6
          # wildcards don't collide on Linux.
          servers = ::Socket.tcp_server_sockets(lookup_host, port)
          raise ::Socket::ResolutionError, "no addresses for #{host.inspect}" if servers.empty?

          actual_port  = servers.first.local_address.ip_port
          display_host = host == "*" ? "*" : (lookup_host || "*")
          host_part    = display_host.include?(":") ? "[#{display_host}]" : display_host
          resolved     = "tcp://#{host_part}:#{actual_port}"
          Listener.new(resolved, servers, actual_port, engine)
        end


        # Creates a TCP dialer for an endpoint.
        #
        # @param endpoint [String] e.g. "tcp://127.0.0.1:5555"
        # @param engine [Engine]
        # @return [Dialer]
        #
        def dialer(endpoint, engine, **)
          Dialer.new(endpoint, engine)
        end


        # Validates that the endpoint's host can be resolved.
        #
        # @param endpoint [String]
        # @return [void]
        #
        def validate_endpoint!(endpoint)
          host, _port = parse_endpoint(endpoint)
          lookup_host = normalize_bind_host(host)
          Addrinfo.getaddrinfo(lookup_host, nil, nil, :STREAM) if lookup_host
        end


        # Normalizes the bind host:
        #   "*"                    → nil (dual-stack wildcard via AI_PASSIVE)
        #   "" / nil / "localhost" → loopback_host (::1 on IPv6-capable hosts, else 127.0.0.1)
        #   else                   → unchanged
        #
        def normalize_bind_host(host)
          case host
          when "*" then nil
          when nil, "", "localhost" then loopback_host
          else host
          end
        end


        # Normalizes the connect host: "", nil, "*", and "localhost" all
        # map to the loopback host. Everything else is passed through so
        # real hostnames still go through the resolver + Happy Eyeballs.
        #
        def normalize_connect_host(host)
          case host
          when nil, "", "*", "localhost" then loopback_host
          else host
          end
        end


        # Loopback address preference for bind/connect normalization.
        # Returns "::1" when the host has at least one non-loopback,
        # non-link-local IPv6 address, otherwise "127.0.0.1".
        #
        def loopback_host
          @loopback_host ||= begin
            has_ipv6 = ::Socket.getifaddrs.any? do |ifa|
              addr = ifa.addr
              addr&.ipv6? && !addr.ipv6_loopback? && !addr.ipv6_linklocal?
            end
            has_ipv6 ? "::1" : "127.0.0.1"
          end
        end


        # Connect timeout: cap each attempt at the reconnect interval so a
        # hung connect(2) (e.g. macOS kqueue + IPv6 ECONNREFUSED not delivered)
        # doesn't block the retry loop. Floor at 0.5s for real-network latency.
        #
        def connect_timeout(options)
          ri = options.reconnect_interval
          ri = ri.end if ri.is_a?(Range)
          [ri, 0.5].max
        end


        # Parses a TCP endpoint URI into host and port.
        #
        # @param endpoint [String]
        # @return [Array(String, Integer)]
        #
        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint)
          [uri.hostname, uri.port]
        end


        # Applies SO_SNDBUF / SO_RCVBUF to +sock+ from the socket's
        # {Options}. No-op when both are nil (OS default).
        #
        # @param sock [Socket, TCPSocket]
        # @param options [Options]
        #
        def apply_buffer_sizes(sock, options)
          if options.sndbuf
            sock.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_SNDBUF, options.sndbuf)
          end

          if options.rcvbuf
            sock.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_RCVBUF, options.rcvbuf)
          end
        end
      end


      # A TCP dialer — stateful factory for outgoing connections.
      #
      # Created once per {Engine#connect}, stored in +@dialers[endpoint]+.
      # Reconnect calls {#connect} directly — no transport lookup or opts
      # replay needed.
      #
      class Dialer
        # @return [String] the endpoint this dialer connects to
        #
        attr_reader :endpoint


        # @param endpoint [String] e.g. "tcp://127.0.0.1:5555"
        # @param engine [Engine]
        #
        def initialize(endpoint, engine)
          @endpoint = endpoint
          @engine   = engine
        end


        # Establishes a TCP connection to the endpoint.
        #
        # @return [void]
        #
        def connect
          host, port = TCP.parse_endpoint(@endpoint)
          host       = TCP.normalize_connect_host(host)
          sock       = ::Socket.tcp(host, port, connect_timeout: TCP.connect_timeout(@engine.options))
          TCP.apply_buffer_sizes(sock, @engine.options)
          @engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: @endpoint)
        end

      end


      # A bound TCP listener.
      #
      class Listener
        # @return [String] resolved endpoint with actual port
        #
        attr_reader :endpoint

        # @return [Integer] bound port
        #
        attr_reader :port

        # @return [Array<Socket>] bound server sockets
        #
        attr_reader :servers


        # @param endpoint [String] resolved endpoint URI
        # @param servers [Array<Socket>]
        # @param port [Integer] bound port number
        # @param engine [Engine]
        #
        def initialize(endpoint, servers, port, engine)
          @endpoint = endpoint
          @servers  = servers
          @port     = port
          @engine   = engine
          @barrier  = nil
        end


        # Spawns accept loop tasks under +parent_task+.
        # Yields an IO::Stream-wrapped client socket for each accepted connection.
        #
        # @param parent_task [Async::Task]
        # @yieldparam io [IO::Stream::Buffered]
        #
        def start_accept_loops(parent_task)
          @barrier = Async::Barrier.new(parent: parent_task)

          @servers.each do |server|
            annotation = "tcp accept #{server.local_address.inspect_sockaddr}"
            @barrier.async(transient: true, annotation:) do
              loop do
                client, _addr = server.accept
                TCP.apply_buffer_sizes(client, @engine.options)
                Async::Task.current.defer_stop do
                  yield IO::Stream::Buffered.wrap(client)
                end
              rescue *AcceptLoop::RECOVERABLE_ERRORS => error
                client&.close rescue nil
                AcceptLoop.recover(error, @engine, @endpoint)
              end
            rescue Async::Stop
            rescue IOError
              # server closed
            rescue => e
              $stderr.write("omq: tcp accept: #{e.class}: #{e.message}\n#{e.backtrace&.first}\n") if OMQ::DEBUG
            ensure
              server.close rescue nil
            end
          end
        end


        # Stops the listener and closes all server sockets.
        #
        # @return [void]
        #
        def stop
          @barrier&.stop
          @servers.each { |s| s.close rescue nil }
        end

      end
    end
  end
end
