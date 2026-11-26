# frozen_string_literal: true

require "socket"
require "uri"
require "set"

module OMQ
  module Transport
    # UDP transport for RADIO/DISH sockets.
    #
    # Connectionless, datagram-based. No ZMTP handshake.
    # DISH binds and receives; RADIO connects and sends.
    #
    # Wire format per datagram:
    #   flags (1 byte = 0x01) | group_size (1 byte) | group (n bytes) | body
    #
    module UDP
      Engine.transports["udp"] = self


      MAX_DATAGRAM_SIZE = 65507

      class << self
        # Creates a bound UDP listener for a DISH socket.
        #
        # @param endpoint [String] e.g. "udp://*:5555" or "udp://127.0.0.1:5555"
        # @param engine [Engine]
        # @return [Listener]
        #
        def listener(endpoint, engine, **)
          host, port = parse_endpoint(endpoint)
          host = "0.0.0.0" if host == "*"
          socket = UDPSocket.new
          socket.bind(host, port)
          Listener.new(endpoint, socket, engine)
        end


        # Creates a UDP dialer for a RADIO socket.
        #
        # @param endpoint [String] e.g. "udp://127.0.0.1:5555"
        # @param engine [Engine]
        # @return [Dialer]
        #
        def dialer(endpoint, engine, **)
          Dialer.new(endpoint, engine)
        end


        # @param endpoint [String]
        # @return [Array(String, Integer)]
        #
        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint)
          [uri.hostname, uri.port]
        end


        # Encodes a group + body into a UDP datagram.
        #
        # @param group [String]
        # @param body [String]
        # @return [String] binary datagram
        #
        def encode_datagram(group, body)
          g = group.b
          b = body.b
          [0x01, g.bytesize].pack("CC") + g + b
        end


        # Sizes the send buffer of a RADIO socket. Uses +sndbuf+ if set;
        # otherwise makes sure a maximum-size datagram fits (macOS
        # defaults to 9216 bytes and refuses larger sends).
        #
        # @param socket [UDPSocket]
        # @param options [Options]
        #
        def apply_send_buffer(socket, options)
          size = options.sndbuf
          size ||= MAX_DATAGRAM_SIZE unless socket.getsockopt(::Socket::SOL_SOCKET, ::Socket::SO_SNDBUF).int >= MAX_DATAGRAM_SIZE
          socket.setsockopt(::Socket::SOL_SOCKET, ::Socket::SO_SNDBUF, size) if size
        rescue SystemCallError
          # keep the OS default
        end


        # Decodes a UDP datagram into [group, body].
        #
        # @param data [String] raw datagram bytes
        # @return [Array(String, String), nil] nil if malformed
        #
        def decode_datagram(data)
          return nil if data.bytesize < 2
          return nil unless data.getbyte(0) & 0x01 == 0x01
          gs = data.getbyte(1)
          return nil if data.bytesize < 2 + gs
          group = data.byteslice(2, gs)
          body  = data.byteslice(2 + gs, data.bytesize - 2 - gs)
          [group, body]
        end
      end


      # A UDP dialer — registers a single RadioConnection with the engine.
      #
      # UDP is connectionless; #connect is a one-shot registration.
      # The reconnect loop will break out after the first successful call.
      #
      class Dialer
        # @return [String] the endpoint this dialer sends to
        #
        attr_reader :endpoint


        # @param endpoint [String]
        # @param engine [Engine]
        #
        def initialize(endpoint, engine)
          @endpoint = endpoint
          @engine   = engine
        end


        # Registers a RadioConnection with the engine.
        #
        # @return [void]
        #
        def connect
          host, port = UDP.parse_endpoint(@endpoint)
          socket     = UDPSocket.new
          UDP.apply_send_buffer(socket, @engine.options)
          conn       = RadioConnection.new(socket, host, port)
          @engine.connection_ready(conn, endpoint: @endpoint)
        end

      end


      # Outgoing UDP connection for RADIO sockets.
      #
      # Intentionally does not implement #read_frame — this signals
      # Routing::Radio to skip the group listener and use ANY_GROUPS.
      #
      class RadioConnection
        # @param socket [UDPSocket]
        # @param host [String]
        # @param port [Integer]
        #
        def initialize(socket, host, port)
          @socket = socket
          @host   = host
          @port   = port
        end


        # Encodes and sends a datagram.
        #
        # @param parts [Array<String>] [group, body]
        #
        # Drops what UDP can't carry: a datagram over
        # {MAX_DATAGRAM_SIZE}, or one the OS refuses as too large.
        #
        def write_message(parts)
          group, body = parts
          datagram = UDP.encode_datagram(group.to_s, body.to_s)
          return if datagram.bytesize > MAX_DATAGRAM_SIZE

          @socket.send(datagram, 0, @host, @port)
        rescue Errno::ECONNREFUSED, Errno::ENETUNREACH, Errno::EMSGSIZE
          # UDP fire-and-forget — drop silently
        end

        alias send_message write_message

        # No-op; UDP datagrams are sent immediately.
        def flush
        end


        # Whether this connection uses CURVE encryption.
        #
        # @return [Boolean] always false
        def curve?
          false
        end


        # Closes the underlying UDP socket.
        def close
          @socket.close rescue nil
        end
      end


      # Incoming UDP connection for DISH sockets.
      #
      # Tracks joined groups locally. JOIN/LEAVE commands from the
      # DISH routing strategy are intercepted via #send_command and
      # never transmitted on the wire.
      #
      class DishConnection
        # @param socket [UDPSocket] bound socket
        #
        def initialize(socket)
          @socket = socket
          @groups = Set.new
        end


        # Receives one matching datagram, blocking until available.
        #
        # Async-safe: rescues IO::WaitReadable and yields to the
        # fiber scheduler via #wait_readable.
        #
        # @return [Array<String>] [group, body], both binary-frozen
        #
        def receive_message
          loop do
            data, = @socket.recvfrom_nonblock(MAX_DATAGRAM_SIZE)
            parts = UDP.decode_datagram(data)
            next unless parts
            group, body = parts
            next unless @groups.include?(group.b)
            return [group, body]
          rescue IO::WaitReadable
            @socket.wait_readable
            retry
          end
        end


        # Handles JOIN/LEAVE commands locally; nothing is sent on the wire.
        #
        # @param cmd [Protocol::ZMTP::Codec::Command]
        #
        def send_command(cmd)
          case cmd.name
          when "JOIN"
            @groups << cmd.data.b.freeze
          when "LEAVE"
            @groups.delete(cmd.data.b)
          end
        end


        # Whether this connection uses CURVE encryption.
        #
        # @return [Boolean] always false
        def curve?
          false
        end


        # Closes the underlying UDP socket.
        def close
          @socket.close rescue nil
        end
      end


      # Bound UDP listener for DISH sockets.
      #
      # Unlike TCP/IPC listeners, there is no accept loop — a single
      # DishConnection is registered directly with the engine, bypassing
      # the ZMTP handshake path.
      #
      class Listener
        # @return [String] bound endpoint
        #
        attr_reader :endpoint

        # @param endpoint [String]
        # @param socket [UDPSocket]
        # @param engine [Engine]
        #
        def initialize(endpoint, socket, engine)
          @endpoint = endpoint
          @socket   = socket
          @engine   = engine
        end


        # Registers a DishConnection with the engine.
        # The on_accepted block is intentionally ignored — no ZMTP handshake.
        #
        # @param parent_task [Async::Task] (unused)
        #
        def start_accept_loops(parent_task, &_on_accepted)
          conn = DishConnection.new(@socket)
          @engine.connection_ready(conn, endpoint: @endpoint)
        end


        # Stops the listener.
        #
        def stop
          @socket.close rescue nil
        end
      end
    end
  end
end
