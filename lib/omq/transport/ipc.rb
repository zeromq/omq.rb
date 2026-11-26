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


      # The URI of an abstract namespace endpoint. +URI.parse+ reads the @
      # of "ipc://@name" as the end of an empty userinfo, so its #to_s is
      # "ipc://name", a relative file path. Here the path keeps the @.
      #
      class AbstractURI < URI::Generic
        # @return [String] e.g. "ipc://@name"
        #
        def to_s
          "ipc://#{path}"
        end

      end


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
        # @raise [Errno::EADDRINUSE] if a live socket or a non-socket file
        #   already occupies the path
        #
        def listener(endpoint, engine, **)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)

          # Reject a too-long path before touching the filesystem.
          ::Socket.sockaddr_un(sock_path)

          remove_stale_socket(path) unless abstract?(path)

          server = UNIXServer.new(sock_path)

          Listener.new(endpoint, server, path, engine)
        end


        # Removes a socket file left behind by a process that is gone.
        # A socket that still accepts connections, or a file that isn't
        # a socket, is never replaced.
        #
        # @param path [String] filesystem path
        # @raise [Errno::EADDRINUSE]
        #
        def remove_stale_socket(path)
          return unless File.exist?(path) || File.symlink?(path)

          unless File.socket?(path)
            raise Errno::EADDRINUSE, "#{path} exists and is not a socket"
          end

          probe_socket(path)
          raise Errno::EADDRINUSE, "#{path} is in use"
        rescue Errno::ECONNREFUSED
          File.delete(path)
        rescue Errno::ENOENT
          # vanished meanwhile
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


        # @param endpoint [String] e.g. "ipc:///tmp/my.sock" or "ipc://@abstract"
        # @return [URI::Generic] the endpoint's URI; an {AbstractURI} for an
        #   abstract namespace name
        #
        def endpoint_uri(endpoint)
          path = parse_path(endpoint)
          return URI.parse(endpoint) unless abstract?(path)

          AbstractURI.new("ipc", nil, nil, nil, nil, path, nil, nil, nil)
        end


        private


        # Connects to +path+ and hangs up. Raises ECONNREFUSED if nothing
        # listens there.
        #
        def probe_socket(path)
          UNIXSocket.new(path).close
        rescue Errno::EAGAIN
          # Backlog full: someone is listening.
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

          # Remember which file we created, by absolute path and inode,
          # so #stop never unlinks a file that another socket bound
          # since, even after a chdir.
          unless IPC.abstract?(path)
            @file_path = File.expand_path(path)
            @file_id   = file_id(@file_path)
          end
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
            rescue *AcceptLoop::RECOVERABLE_ERRORS => error
              client&.close rescue nil
              AcceptLoop.recover(error, @engine, @endpoint)
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

          if @file_id && file_id(@file_path) == @file_id
            File.delete(@file_path) rescue nil
          end
        end


        private


        def file_id(path)
          stat = File.lstat(path)
          [stat.dev, stat.ino]
        rescue SystemCallError
          nil
        end

      end
    end
  end
end
