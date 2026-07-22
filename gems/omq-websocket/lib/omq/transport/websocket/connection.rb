# frozen_string_literal: true

require "protocol/zmtp"
require "async/clock"

module OMQ
  module Transport
    module WebSocket
      # Quacks like Protocol::ZMTP::Connection but speaks ZWS 2.0 over
      # an Async::WebSocket::Connection (or any object that responds to
      # +#read+, +#send_binary+, +#flush+, +#close+, +#protocol+).
      #
      # The HTTP/WS upgrade is already complete by the time this is
      # constructed — the Engine instantiates this class via
      # OMQ::Transport::WebSocket.connection_class once
      # +handle_accepted+ / +handle_connected+ delivers the ws_conn.
      #
      # No ZMTP/3.1 greeting. The mechanism (NULL or no-mechanism) is
      # determined from the negotiated Sec-WebSocket-Protocol. Identity
      # is exchanged either via a READY command (NULL) or as the first
      # data message (no-mechanism), per RFC 45.
      class Connection

        attr_reader :peer_socket_type
        attr_reader :peer_identity
        attr_reader :peer_public_key
        attr_reader :peer_properties
        attr_reader :peer_major
        attr_reader :peer_minor
        attr_reader :last_received_at
        attr_reader :ws


        # @param ws [#read, #send_binary, #flush, #close, #protocol] WebSocket connection
        # @param socket_type [String] our socket type (e.g. "REQ")
        # @param identity [String] our identity
        # @param as_server [Boolean] true on accepted side
        # @param mechanism [Object, nil] ignored — ZWS uses the negotiated subprotocol instead
        # @param max_message_size [Integer, nil] max frame body size
        # @param opts [Hash] extra READY properties (NULL handshake only)
        #
        def initialize(ws, socket_type:, identity: "", as_server: false,
                       mechanism: nil, max_message_size: nil, **opts)
          @ws               = ws
          @socket_type      = socket_type.to_s
          @identity         = identity || ""
          @as_server        = as_server
          @max_message_size = max_message_size
          @metadata         = opts.empty? ? nil : opts.transform_keys(&:to_s)

          @peer_socket_type = nil
          @peer_identity    = nil
          @peer_public_key  = nil
          @peer_properties  = nil

          # Advertise ZMTP 3.1 to upper layers so SUBSCRIBE/CANCEL always
          # go via send_command (the ZWS form per RFC 45) rather than the
          # legacy in-band byte-prefix form.
          @peer_major       = 3
          @peer_minor       = 1

          @last_received_at = nil
          @mutex            = Mutex.new
          @closed           = false
        end


        def encrypted?
          false
        end


        def curve?
          false
        end


        # Performs the ZWS 2.0 mechanism handshake. Branches on the
        # subprotocol negotiated during the HTTP upgrade.
        #
        # @return [void]
        # @raise [Protocol::ZMTP::Error] on protocol violation, unsupported
        #   subprotocol, or incompatible peer socket type
        #
        def handshake!
          case @ws.protocol
          when "ZWS2.0/NULL"
            handshake_null!
            validate_peer_compatibility!
          when "ZWS2.0", nil
            handshake_no_mechanism!
            # No socket-type validation possible without READY exchange.
          else
            raise Protocol::ZMTP::Error, "unsupported ZWS subprotocol: #{@ws.protocol.inspect}"
          end
        end


        def send_message(parts)
          @mutex.synchronize do
            write_frames(parts)
            @ws.flush
          end
        end


        def write_message(parts)
          @mutex.synchronize do
            write_frames(parts)
          end
        end


        def write_messages(messages)
          @mutex.synchronize do
            messages.each { |parts| write_frames(parts) }
          end
        end


        def flush
          @mutex.synchronize do
            @ws.flush
          end
        end


        def send_command(command)
          @mutex.synchronize do
            @ws.send_binary(Codec.encode(command.to_body, command: true))
            @ws.flush
          end
        end


        # Reads a multi-frame message. Auto-handles PING/PONG and yields
        # any other command frames to the caller block (matching
        # Protocol::ZMTP::Connection#receive_message semantics).
        #
        # @return [Array<String>] frame bodies
        # @raise [EOFError] on peer close
        #
        def receive_message
          frames = []

          loop do
            frame = read_frame

            if frame.command?
              yield frame if block_given?
              next
            end

            frames << frame.body
            break unless frame.more?
          end

          frames
        end


        # Reads one ZWS frame. Auto-handles PING (replies PONG) and
        # discards PONG. Returns a Protocol::ZMTP::Codec::Frame so
        # upstream code (Subscription.parse, fan_out subscription
        # listeners) sees the same shape as ZMTP/3.1.
        #
        # @return [Protocol::ZMTP::Codec::Frame]
        # @raise [EOFError] on peer close
        #
        def read_frame
          loop do
            message = @ws.read
            unless message
              close
              raise EOFError, "ZWS connection closed"
            end

            bytes = message.buffer
            bytes = bytes.b if bytes.encoding != Encoding::BINARY
            flag, body = Codec.decode(bytes)

            if @max_message_size && body.bytesize > @max_message_size
              raise Protocol::ZMTP::Error,
                    "ZWS frame exceeds max_message_size (#{body.bytesize} > #{@max_message_size})"
            end

            command = (flag & Codec::FLAG_COMMAND) != 0
            more    = !command && (flag & Codec::FLAG_MORE) != 0
            frame   = Protocol::ZMTP::Codec::Frame.new(body, more: more, command: command)

            touch_heartbeat

            if frame.command?
              cmd = Protocol::ZMTP::Codec::Command.from_body(frame.body)
              case cmd.name
              when "PING"
                _ttl, context = cmd.ping_ttl_and_context
                send_command(Protocol::ZMTP::Codec::Command.pong(context: context))
                next
              when "PONG"
                next
              end
            end

            return frame
          end
        end


        def touch_heartbeat
          @last_received_at = Async::Clock.now
        end


        def heartbeat_expired?(timeout)
          return false unless @last_received_at
          (Async::Clock.now - @last_received_at) > timeout
        end


        def close
          return if @closed
          @closed = true
          @ws.close
        rescue IOError, ::Protocol::WebSocket::ClosedError
          # already closed
        end


        def closed?
          @closed
        end


        private


        def write_frames(parts)
          last = parts.size - 1

          parts.each_with_index do |part, i|
            @ws.send_binary(Codec.encode(part, more: i < last))
          end
        end


        def handshake_null!
          ready = Protocol::ZMTP::Codec::Command.ready(
            socket_type: @socket_type,
            identity:    @identity,
            metadata:    @metadata,
          )
          send_command(ready)

          frame = read_command_frame!
          cmd   = Protocol::ZMTP::Codec::Command.from_body(frame.body)

          unless cmd.name == "READY"
            raise Protocol::ZMTP::Error, "expected READY, got #{cmd.name}"
          end

          props             = cmd.properties
          @peer_properties  = props
          @peer_socket_type = props["Socket-Type"]
          @peer_identity    = props["Identity"] || Codec::EMPTY_BINARY

          unless @peer_socket_type
            raise Protocol::ZMTP::Error, "peer READY missing Socket-Type"
          end
        end


        def handshake_no_mechanism!
          @mutex.synchronize do
            @ws.send_binary(Codec.encode(@identity || Codec::EMPTY_BINARY))
            @ws.flush
          end

          message = @ws.read
          unless message
            raise Protocol::ZMTP::Error, "ZWS peer closed before sending identity"
          end

          bytes = message.buffer
          bytes = bytes.b if bytes.encoding != Encoding::BINARY
          _flag, body    = Codec.decode(bytes)
          @peer_identity = body

          touch_heartbeat
        end


        def read_command_frame!
          loop do
            message = @ws.read
            unless message
              raise Protocol::ZMTP::Error, "ZWS peer closed during handshake"
            end

            bytes = message.buffer
            bytes = bytes.b if bytes.encoding != Encoding::BINARY
            flag, body = Codec.decode(bytes)

            next if (flag & Codec::FLAG_COMMAND) == 0

            touch_heartbeat
            return Protocol::ZMTP::Codec::Frame.new(body, command: true)
          end
        end


        def validate_peer_compatibility!
          unless Protocol::ZMTP::VALID_PEERS[@socket_type.to_sym]&.include?(@peer_socket_type.to_sym)
            raise Protocol::ZMTP::Error,
                  "incompatible socket types: #{@socket_type} cannot connect to #{@peer_socket_type}"
          end
        end

      end
    end
  end
end
