# frozen_string_literal: true

require "async"
require "async/queue"
require_relative "inproc/pipe"

module OMQ
  module Transport
    # Ruby in-process transport (ruby:// scheme).
    #
    # Both peers are Ruby backend sockets in the same process. Messages
    # are transferred as Ruby arrays — no ZMTP framing, no byte
    # serialization. Parts are already frozen by Writable#send, so the
    # receiver sees the same immutable contract as ZMTP transports.
    #
    # The inproc:// scheme is reserved for native backends (FFI/libzmq,
    # Rust/omq-tokio) which have their own in-process registries.
    #
    module Inproc
      Engine.transports["ruby"] = self


      # Socket types that exchange commands (SUBSCRIBE/CANCEL) over inproc.
      #
      COMMAND_TYPES = %i[PUB SUB XPUB XSUB RADIO DISH].freeze


      # Global registry of bound inproc endpoints.
      #
      @registry = {}
      @mutex    = Mutex.new
      @waiters  = Hash.new { |h, k| h[k] = [] }


      class << self
        # @return [Hash{String => Engine}] bound inproc endpoints
        #
        attr_reader :registry


        # Creates a bound inproc listener.
        #
        # @param endpoint [String] e.g. "ruby://my-endpoint"
        # @param engine [Engine] the owning engine
        # @return [Listener]
        # @raise [ArgumentError] if endpoint is already bound
        #
        def listener(endpoint, engine, **)
          @mutex.synchronize do
            if @registry.key?(endpoint)
              raise ArgumentError, "endpoint already bound: #{endpoint}"
            end

            @registry[endpoint] = engine

            # Wake any pending connects
            @waiters[endpoint].each { |p| p.resolve(true) }
            @waiters.delete(endpoint)
          end
          Listener.new(endpoint)
        end


        # Connects to a bound inproc endpoint.
        #
        # @param endpoint [String] e.g. "ruby://my-endpoint"
        # @param engine [Engine] the connecting engine
        # @return [void]
        #
        def connect(endpoint, engine, **)
          bound_engine = @mutex.synchronize { @registry[endpoint] }
          bound_engine ||= await_bind(endpoint, engine) or return
          establish_link(engine, bound_engine, endpoint)
        end


        # Removes a bound endpoint from the registry.
        #
        # @param endpoint [String]
        # @return [void]
        #
        def unbind(endpoint)
          @mutex.synchronize { @registry.delete(endpoint) }
        end


        # Resets the registry. Used in tests.
        #
        # @return [void]
        #
        def reset!
          @mutex.synchronize do
            @registry.clear
            @waiters.clear
          end
        end


        private


        # Wires up a client-server inproc pipe pair after validating
        # that the two socket types are compatible.
        #
        # @param client_engine [Engine] the connecting engine
        # @param server_engine [Engine] the bound engine
        # @param endpoint [String] the inproc endpoint name
        #
        def establish_link(client_engine, server_engine, endpoint)
          client_type = client_engine.socket_type
          server_type = server_engine.socket_type

          unless Protocol::ZMTP::VALID_PEERS[client_type]&.include?(server_type)
            raise Protocol::ZMTP::Error,
                  "incompatible socket types: #{client_type} cannot connect to #{server_type}"
          end

          needs_cmds = needs_commands?(client_engine, server_engine, client_type, server_type)
          client_pipe, server_pipe = make_pipe_pair client_engine, server_engine,
                                                    client_type, server_type, needs_cmds

          client_engine.connection_ready(client_pipe, endpoint: endpoint)
          server_engine.connection_ready(server_pipe, endpoint: endpoint)
        end


        # Decides whether a Pipe pair needs command queues.
        # Pipe's fast path skips queues entirely; command queues
        # are only needed for socket types that exchange ZMTP commands
        # (e.g. ROUTER/DEALER identity, PUB/SUB subscriptions) or when
        # either side enables QoS ≥ 1.
        #
        # @return [Boolean]
        #
        def needs_commands?(ce, se, ct, st)
          return true if COMMAND_TYPES.include?(ct) || COMMAND_TYPES.include?(st)
          return true if qos_enabled?(ce.options) || qos_enabled?(se.options)
          false
        end


        # QoS integration: core +Options#qos+ defaults to Integer +0+.
        # When the omq-qos extension is loaded, +#qos+ holds either
        # +nil+ (QoS 0) or an +OMQ::QoS+ instance (levels 1–3). Treat
        # both Integer 0 and nil as disabled.
        def qos_enabled?(options)
          q = options.qos
          return false if q.nil?
          return q != 0 if q.is_a?(Integer)
          true
        end


        # Builds a bidirectional {Pipe} pair for client + server.
        # When +needs_cmds+ is false the pipes have no command queues
        # (fast path — all traffic bypasses Async::Queue entirely).
        #
        # @return [Array(Pipe, Pipe)] client, server
        #
        def make_pipe_pair(ce, se, ct, st, needs_cmds)
          if needs_cmds
            a_to_b = Async::Queue.new
            b_to_a = Async::Queue.new
          end

          client = Pipe.new(send_queue: needs_cmds ? a_to_b : nil,
                            receive_queue: needs_cmds ? b_to_a : nil,
                            peer_identity: se.options.identity, peer_type: st.to_s)
          server = Pipe.new(send_queue: needs_cmds ? b_to_a : nil,
                            receive_queue: needs_cmds ? a_to_b : nil,
                            peer_identity: ce.options.identity, peer_type: ct.to_s)

          client.peer = server
          server.peer = client
          [client, server]
        end


        def await_bind(endpoint, engine)
          # Endpoint not bound yet — wait briefly then start background retry.
          # Matches ZMQ 4.x: connect to unbound inproc succeeds silently.
          ri      = engine.options.reconnect_interval
          timeout = ri.is_a?(Range) ? ri.begin : ri
          promise = Async::Promise.new

          @mutex.synchronize do
            @waiters[endpoint] << promise
          end

          if promise.wait?(timeout: timeout)
            @mutex.synchronize do
              @registry[endpoint]
            end
          else
            @mutex.synchronize do
              @waiters[endpoint].delete(promise)
            end

            start_connect_retry(endpoint, engine)
            nil
          end
        end


        # Spawns a background task that periodically retries
        # #establish_link until the endpoint appears in the registry.
        #
        # @param endpoint [String] the inproc endpoint name
        # @param engine [Engine] the connecting engine
        #
        def start_connect_retry(endpoint, engine)
          engine.spawn_inproc_retry(endpoint) do |ivl|
            loop do
              sleep ivl
              bound_engine = @mutex.synchronize { @registry[endpoint] }

              if bound_engine
                establish_link(engine, bound_engine, endpoint)
                break
              end
            end
          end
        end
      end


      # A bound inproc endpoint handle.
      #
      class Listener
        # @return [String] the bound endpoint
        #
        attr_reader :endpoint


        # @param endpoint [String]
        #
        def initialize(endpoint)
          @endpoint = endpoint
        end


        # Stops the listener by removing it from the registry.
        #
        # @return [void]
        #
        def stop
          Inproc.unbind(@endpoint)
        end
      end

    end
  end
end
