# frozen_string_literal: true

require "socket"
require "uri"
require "io/stream"
require "omq"

require_relative "connection"

module OMQ
  module Transport
    module Lz4Tcp
      SCHEME = "lz4+tcp"

      class << self
        # Called by OMQ::Engine::ConnectionLifecycle after the ZMTP
        # handshake completes; we return the default connection class
        # so that handshake itself runs uncompressed over raw TCP.
        def connection_class
          Protocol::ZMTP::Connection
        end


        # Creates a bound lz4+tcp listener.
        #
        # @param endpoint [String] e.g. "lz4+tcp://127.0.0.1:0"
        # @param engine [OMQ::Engine]
        # @param dict [String, nil] user-supplied dictionary bytes to
        #   ship on the first outgoing message. If nil, no dict is
        #   shipped (payloads compress without dict or go plaintext
        #   below the min-size threshold).
        # @param auto_dict [true, Hash, nil] enable automatic dictionary
        #   training from early traffic (RFC §7.6). Pass `true` for
        #   defaults (capacity=2048, trigger=100) or a Hash with
        #   `:capacity` and/or `:trigger` keys. Mutually exclusive
        #   with `dict:`.
        # @return [Listener]
        def listener(endpoint, engine, dict: nil, auto_dict: nil, **)
          validate_dict!(dict)
          validate_auto_dict!(auto_dict, dict)

          host, port  = parse_endpoint(endpoint)
          lookup_host = normalize_bind_host(host)
          servers     = ::Socket.tcp_server_sockets(lookup_host, port)

          if servers.empty?
            raise ::Socket::ResolutionError, "no addresses for #{host.inspect}"
          end

          actual_port  = servers.first.local_address.ip_port
          display_host = host == "*" ? "*" : (lookup_host || "*")
          host_part    = display_host.include?(":") ? "[#{display_host}]" : display_host
          resolved     = "#{SCHEME}://#{host_part}:#{actual_port}"

          Listener.new(resolved, servers, actual_port, engine, dict&.b, normalize_auto_dict(auto_dict))
        end


        # Creates an lz4+tcp dialer for an endpoint.
        #
        # @param endpoint [String]
        # @param engine [OMQ::Engine]
        # @param dict [String, nil] user-supplied dictionary bytes.
        # @param auto_dict [true, Hash, nil] enable automatic dictionary
        #   training. See `listener` for details.
        # @return [Dialer]
        def dialer(endpoint, engine, dict: nil, auto_dict: nil, **)
          validate_dict!(dict)
          validate_auto_dict!(auto_dict, dict)
          Dialer.new(endpoint, engine, dict&.b, normalize_auto_dict(auto_dict))
        end


        def validate_endpoint!(endpoint)
          host, _port = parse_endpoint(endpoint)
          lookup_host = normalize_connect_host(host)
          Addrinfo.getaddrinfo(lookup_host, nil, nil, :STREAM) if lookup_host
        end


        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint)
          [uri.hostname, uri.port]
        end


        def normalize_bind_host(host)
          case host
          when "*" then nil
          when nil, "", "localhost" then TCP.loopback_host
          else host
          end
        end


        def normalize_connect_host(host)
          case host
          when nil, "", "*", "localhost" then TCP.loopback_host
          else host
          end
        end


        def connect_timeout(options)
          ri = options.reconnect_interval
          ri = ri.end if ri.is_a?(Range)
          [ri, 0.5].max
        end


        private


        def validate_dict!(dict)
          return if dict.nil?

          size = dict.bytesize
          return if size >= 1 && size <= LZ4::Codec::MAX_DICT_SIZE

          raise LZ4::ProtocolError,
            "dict size #{size} out of range [1, #{LZ4::Codec::MAX_DICT_SIZE}]"
        end


        def validate_auto_dict!(auto_dict, dict)
          return unless auto_dict

          if dict
            raise ArgumentError, "cannot combine auto_dict: and dict:"
          end

          return if auto_dict == true

          unless auto_dict.is_a?(Hash)
            raise TypeError, "auto_dict: must be true or a Hash; got #{auto_dict.class}"
          end

          cap = auto_dict[:capacity]
          if cap && (cap < 1 || cap > LZ4::Codec::MAX_DICT_SIZE)
            raise ArgumentError,
              "auto_dict capacity #{cap} out of range [1, #{LZ4::Codec::MAX_DICT_SIZE}]"
          end
        end


        def normalize_auto_dict(auto_dict)
          return unless auto_dict

          capacity = Lz4Connection::DEFAULT_DICT_CAPACITY
          trigger  = Lz4Connection::DEFAULT_TRAIN_TRIGGER
          if auto_dict.is_a?(Hash)
            capacity = auto_dict[:capacity] if auto_dict[:capacity]
            trigger  = auto_dict[:trigger]  if auto_dict[:trigger]
          end
          { capacity: capacity, trigger: trigger }.freeze
        end
      end


      # Dialer: outgoing connections.
      class Dialer
        attr_reader :endpoint

        def initialize(endpoint, engine, dict_bytes, auto_dict)
          @endpoint  = endpoint
          @engine    = engine
          @dict_bytes = dict_bytes
          @auto_dict = auto_dict
        end


        def connect
          host, port = Lz4Tcp.parse_endpoint(@endpoint)
          host       = Lz4Tcp.normalize_connect_host(host)
          sock       = ::Socket.tcp(host, port, connect_timeout: Lz4Tcp.connect_timeout(@engine.options))

          TCP.apply_buffer_sizes(sock, @engine.options)

          @engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: @endpoint)
        rescue
          sock&.close
          raise
        end


        def wrap_connection(conn)
          Lz4Connection.new(
            conn,
            send_dict_bytes:  @dict_bytes,
            max_message_size: @engine.options.max_message_size,
            auto_dict:        @auto_dict,
          )
        end
      end


      # Listener: bound server accepting incoming connections.
      class Listener
        attr_reader :endpoint, :port

        def initialize(endpoint, servers, port, engine, dict_bytes, auto_dict)
          @endpoint  = endpoint
          @servers   = servers
          @port      = port
          @engine    = engine
          @dict_bytes = dict_bytes
          @auto_dict = auto_dict
          @tasks     = []
        end


        def wrap_connection(conn)
          Lz4Connection.new(
            conn,
            send_dict_bytes:  @dict_bytes,
            max_message_size: @engine.options.max_message_size,
            auto_dict:        @auto_dict,
          )
        end


        def start_accept_loops(parent_task, &on_accepted)
          @tasks = @servers.map do |server|
            parent_task.async(transient: true, annotation: "#{SCHEME} accept #{@endpoint}") do
              loop do
                client, _addr = server.accept

                Async::Task.current.defer_stop do
                  TCP.apply_buffer_sizes(client, @engine.options)

                  stream = IO::Stream::Buffered.wrap(client)

                  on_accepted.call(stream)
                end
              end
            rescue Async::Stop
            rescue IOError
            ensure
              server.close rescue nil
            end
          end
        end


        def stop
          @tasks.each(&:stop)
          @servers.each { |s| s.close rescue nil }
        end
      end

      Engine.transports[SCHEME] = self
    end
  end
end
