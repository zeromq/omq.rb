# frozen_string_literal: true

require "socket"
require "uri"
require "io/stream"

module OMQ
  module Transport
    module ZstdTcp
      SCHEME = "zstd+tcp"


      # WeakKeyMap keyed by Engine — the Codec for a socket lives as long
      # as the Engine (and therefore the socket) lives.  When the socket is
      # closed and the Engine is GC'd, the entry disappears automatically.
      #
      @codecs = ObjectSpace::WeakKeyMap.new


      class << self
        # Creates a bound zstd+tcp listener.
        #
        # @param endpoint [String] e.g. "zstd+tcp://127.0.0.1:5555"
        # @param engine [Engine]
        # @param level [Integer] Zstd compression level
        # @param dict [String, nil] user-supplied dictionary bytes
        # @return [Listener]
        #
        def listener(endpoint, engine, level: -3, dict: nil, **)
          codec = codec_for(engine, level: level, dict: dict)

          host, port = parse_endpoint(endpoint)
          host       = normalize_bind_host(host)
          servers    = ::Socket.tcp_server_sockets(host, port)

          if servers.empty?
            raise ::Socket::ResolutionError, "no addresses for #{host.inspect}"
          end

          actual_port  = servers.first.local_address.ip_port
          display_host = host || "*"
          host_part    = display_host.include?(":") ? "[#{display_host}]" : display_host
          resolved     = "#{SCHEME}://#{host_part}:#{actual_port}"

          Listener.new(resolved, servers, actual_port, engine, codec)
        end


        # Creates a zstd+tcp dialer for an endpoint.
        #
        # @param endpoint [String] e.g. "zstd+tcp://127.0.0.1:5555"
        # @param engine [Engine]
        # @param level [Integer] Zstd compression level
        # @param dict [String, nil] user-supplied dictionary bytes
        # @return [Dialer]
        #
        def dialer(endpoint, engine, level: -3, dict: nil, **)
          codec = codec_for(engine, level: level, dict: dict)
          Dialer.new(endpoint, engine, codec)
        end


        def validate_endpoint!(endpoint)
          host, _port = parse_endpoint(endpoint)
          host = normalize_connect_host(host)
          Addrinfo.getaddrinfo(host, nil, nil, :STREAM) if host
        end


        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint)
          [uri.hostname, uri.port]
        end


        def normalize_bind_host(host)
          return nil if host == "*"
          host
        end


        def normalize_connect_host(host)
          host == "*" ? "127.0.0.1" : host
        end


        def connect_timeout(options)
          ri = options.reconnect_interval
          ri.is_a?(Range) ? ri.end : [ri * 10, 30].min
        end


        private


        # Returns the shared Codec for this engine (socket), creating one
        # on first call.  Stored in a WeakKeyMap keyed by engine — the
        # entry is automatically removed when the engine is GC'd.
        #
        # Inside a Ractor the module-level @codecs ivar is inaccessible,
        # so we fall back to creating a fresh Codec per call.  This is
        # fine: each Ractor worker owns a single socket with one endpoint,
        # so there is nothing to share.
        #
        # @param engine [Engine]
        # @param level [Integer]
        # @param dict [String, nil]
        # @return [Codec]
        #
        def codec_for(engine, level:, dict:)
          @codecs[engine] ||= Codec.new level: level, dict: dict,
            max_message_size: engine.options.max_message_size

        rescue Ractor::IsolationError
          Codec.new level: level, dict: dict,
            max_message_size: engine.options.max_message_size
        end
      end


      # A zstd+tcp dialer — stateful factory for outgoing connections.
      #
      # Holds the Codec (compression cache, training state, dict) and
      # wraps new connections with {ZstdConnection}.
      #
      class Dialer
        # @return [String] the endpoint this dialer connects to
        #
        attr_reader :endpoint


        # @param endpoint [String]
        # @param engine [Engine]
        # @param codec [Codec]
        #
        def initialize(endpoint, engine, codec)
          @endpoint = endpoint
          @engine   = engine
          @codec    = codec
        end


        # Establishes a TCP connection to the endpoint.
        #
        # @return [void]
        #
        def connect
          host, port = ZstdTcp.parse_endpoint(@endpoint)
          host       = ZstdTcp.normalize_connect_host(host)
          sock       = ::Socket.tcp host, port, connect_timeout: ZstdTcp.connect_timeout(@engine.options)

          TCP.apply_buffer_sizes sock, @engine.options

          @engine.handle_connected IO::Stream::Buffered.wrap(sock), endpoint: @endpoint
        rescue
          sock&.close
          raise
        end


        # Wraps a raw ZMTP connection with Zstd compression.
        #
        # @param conn [Protocol::ZMTP::Connection]
        # @return [ZstdConnection]
        #
        def wrap_connection(conn)
          ZstdConnection.new(conn, @codec)
        end

      end


      # A bound zstd+tcp listener.
      #
      class Listener
        # @return [String] resolved endpoint with actual port
        #
        attr_reader :endpoint


        # @return [Integer] bound port
        #
        attr_reader :port


        # @param endpoint [String] resolved endpoint URI
        # @param servers [Array<Socket>]
        # @param port [Integer] bound port number
        # @param engine [Engine]
        # @param codec [Codec]
        #
        def initialize(endpoint, servers, port, engine, codec)
          @endpoint = endpoint
          @servers  = servers
          @port     = port
          @engine   = engine
          @codec    = codec
          @tasks    = []
        end


        # Wraps a raw ZMTP connection with Zstd compression.
        #
        # @param conn [Protocol::ZMTP::Connection]
        # @return [ZstdConnection]
        #
        def wrap_connection(conn)
          ZstdConnection.new(conn, @codec)
        end


        # Spawns accept loop tasks under +parent_task+.
        #
        # @param parent_task [Async::Task]
        # @yieldparam io [IO::Stream::Buffered]
        #
        def start_accept_loops(parent_task, &on_accepted)
          @tasks = @servers.map do |server|
            parent_task.async(transient: true, annotation: "zstd+tcp accept #{@endpoint}") do
              loop do
                client, _addr = server.accept

                Async::Task.current.defer_stop do
                  TCP.apply_buffer_sizes(client, @engine.options)

                  stream = IO::Stream::Buffered.wrap(client)

                  on_accepted.call stream
                end
              end
            rescue Async::Stop
            rescue IOError
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
          @tasks.each(&:stop)
          @servers.each { |s| s.close rescue nil }
        end

      end

    end
  end
end
