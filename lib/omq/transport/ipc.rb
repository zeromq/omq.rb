# frozen_string_literal: true

require "socket"
require "io/stream"

module OMQ
  module Transport
    # IPC transport using Unix domain sockets.
    #
    # Supports both file-based paths and Linux abstract namespace
    # (paths starting with @).
    #
    module IPC
      Engine.transports["ipc"] = self


      class << self
        # ZMTP connection class used for IPC-accepted/dialed peers.
        #
        # @return [Class]
        #
        def connection_class
          Protocol::ZMTP::Connection
        end


        # Creates a bound IPC listener.
        #
        # @param endpoint [String] e.g. "ipc:///tmp/my.sock" or "ipc://@abstract"
        # @param engine [Engine]
        # @return [Listener]
        #
        def listener(endpoint, engine, **)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)

          # Remove stale socket file for file-based paths
          File.delete(sock_path) if !abstract?(path) && File.exist?(sock_path)

          server = UNIXServer.new(sock_path)

          Listener.new(endpoint, server, path, engine)
        end


        # Creates an IPC dialer for an endpoint.
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [Dialer]
        #
        def dialer(endpoint, engine, **)
          Dialer.new(endpoint, engine)
        end


        # Applies SO_SNDBUF / SO_RCVBUF to +sock+ from the socket's
        # {Options}. No-op when both are nil (OS default).
        #
        # @param sock [UNIXSocket, UNIXServer]
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


        # Extracts path from "ipc://path".
        #
        def parse_path(endpoint)
          endpoint.delete_prefix("ipc://")
        end


        # Converts @ prefix to \0 for abstract namespace.
        #
        def to_socket_path(path)
          if abstract?(path)
            "\0#{path[1..]}"
          else
            path
          end
        end


        # @return [Boolean] true if abstract namespace path
        #
        def abstract?(path)
          path.start_with?("@")
        end
      end


      # An IPC dialer — stateful factory for outgoing connections.
      #
      class Dialer
        # @return [String] the endpoint this dialer connects to
        #
        attr_reader :endpoint


        # @param endpoint [String]
        # @param engine [Engine]
        #
        def initialize(endpoint, engine)
          @endpoint = endpoint
          @engine   = engine
        end


        # Establishes a Unix socket connection to the endpoint.
        #
        # @return [void]
        #
        def connect
          path      = IPC.parse_path(@endpoint)
          sock_path = IPC.to_socket_path(path)
          sock      = UNIXSocket.new(sock_path)
          IPC.apply_buffer_sizes(sock, @engine.options)
          @engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: @endpoint)
        end

      end


      # A bound IPC listener.
      #
      class Listener
        # @return [String] the endpoint
        #
        attr_reader :endpoint

        # @return [UNIXServer] the server socket
        #
        attr_reader :server


        # @param endpoint [String] the IPC endpoint URI
        # @param server [UNIXServer]
        # @param path [String] filesystem or abstract namespace path
        # @param engine [Engine]
        #
        def initialize(endpoint, server, path, engine)
          @endpoint = endpoint
          @server   = server
          @path     = path
          @engine   = engine
          @task     = nil
        end


        # Spawns an accept loop task under +parent_task+.
        # Yields an IO::Stream-wrapped client socket for each accepted connection.
        #
        # @param parent_task [Async::Task]
        # @yieldparam io [IO::Stream::Buffered]
        #
        def start_accept_loops(parent_task)
          annotation = "ipc accept #{@endpoint}"
          @task = parent_task.async(transient: true, annotation:) do
            loop do
              client = @server.accept
              IPC.apply_buffer_sizes(client, @engine.options)
              Async::Task.current.defer_stop do
                yield IO::Stream::Buffered.wrap(client)
              end
            end
          rescue Async::Stop
          rescue IOError
            # server closed
          rescue => e
            $stderr.write("omq: ipc accept: #{e.class}: #{e.message}\n#{e.backtrace&.first}\n") if OMQ::DEBUG
          ensure
            @server.close rescue nil
          end
        end


        # Stops the listener and removes the socket file.
        #
        # @return [void]
        #
        def stop
          @task&.stop
          @server.close rescue nil

          # Clean up socket file for file-based paths
          unless @path.start_with?("@")
            File.delete(@path) rescue nil
          end
        end

      end
    end
  end
end
