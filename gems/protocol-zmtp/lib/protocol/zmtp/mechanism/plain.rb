# frozen_string_literal: true

module Protocol
  module ZMTP
    module Mechanism
      # PLAIN security mechanism — username/password authentication, no encryption.
      #
      # Implements the ZMTP PLAIN handshake (RFC 24):
      #
      #   client → server:  HELLO    (username + password)
      #   server → client:  WELCOME  (empty, credentials accepted)
      #   client → server:  INITIATE (socket metadata)
      #   server → client:  READY    (socket metadata)
      #
      class Plain
        MECHANISM_NAME = "PLAIN"


        # @param username [String] client username (max 255 bytes)
        # @param password [String] client password (max 255 bytes)
        # @param authenticator [#call, nil] server-side credential verifier;
        #   called as +authenticator.call(username, password)+ and must return
        #   truthy to accept the connection.  When +nil+, all credentials pass.
        def initialize(username: "", password: "", authenticator: nil)
          @username      = username
          @password      = password
          @authenticator = authenticator
        end


        # Performs the full PLAIN handshake over +io+.
        #
        # @param io [#read_exactly, #write, #flush] transport IO
        # @param as_server [Boolean]
        # @param socket_type [String]
        # @param identity [String]
        # @param metadata [Hash{String => String}, nil] extra READY properties
        # @return [Hash] { peer_socket_type:, peer_identity:, peer_properties: }
        # @raise [Error]
        def handshake!(io, as_server:, socket_type:, identity:, metadata: nil)
          io.write(Codec::Greeting.encode(mechanism: MECHANISM_NAME, as_server: as_server))
          io.flush

          peer_greeting = Codec::Greeting.read_from(io)

          unless peer_greeting[:mechanism] == MECHANISM_NAME
            raise Error, "unsupported mechanism: #{peer_greeting[:mechanism]}"
          end

          if as_server
            server_handshake! io, socket_type: socket_type, identity: identity, metadata: metadata
          else
            client_handshake! io, socket_type: socket_type, identity: identity, metadata: metadata
          end
        end


        # @return [Boolean] false — PLAIN does not encrypt frames
        def encrypted?
          false
        end


        private


        def client_handshake!(io, socket_type:, identity:, metadata: nil)
          send_command(io, hello_command)

          cmd = read_command(io)
          raise Error, "expected WELCOME, got #{cmd.name}" unless cmd.name == "WELCOME"

          props = {
            "Socket-Type" => socket_type,
            "Identity" => identity,
          }
          props.merge!(metadata) if metadata && !metadata.empty?
          initiate = Codec::Command.new("INITIATE", Codec::Command.encode_properties(props))
          send_command(io, initiate)

          cmd = read_command(io)
          raise Error, "expected READY, got #{cmd.name}" unless cmd.name == "READY"

          extract_peer_info(cmd)
        end


        def server_handshake!(io, socket_type:, identity:, metadata: nil)
          cmd = read_command(io)
          raise Error, "expected HELLO, got #{cmd.name}" unless cmd.name == "HELLO"

          username, password = decode_credentials(cmd.data)

          if @authenticator && !@authenticator.call(username, password)
            raise Error, "authentication failed"
          end

          send_command(io, Codec::Command.new("WELCOME"))

          cmd = read_command(io)
          raise Error, "expected INITIATE, got #{cmd.name}" unless cmd.name == "INITIATE"

          peer_info = extract_peer_info(cmd)

          ready = Codec::Command.ready socket_type: socket_type, identity: identity, metadata: metadata
          send_command io, ready

          peer_info
        end


        def hello_command
          u = @username.b
          p = @password.b

          raise Error, "username too long (max 255 bytes)" if u.bytesize > 255
          raise Error, "password too long (max 255 bytes)" if p.bytesize > 255

          data = u.bytesize.chr.b + u + p.bytesize.chr.b + p
          Codec::Command.new("HELLO", data)
        end


        def decode_credentials(data)
          data = data.b
          raise Error, "HELLO body too short" if data.bytesize < 1

          u_len    = data.getbyte(0)
          p_offset = 1 + u_len

          raise Error, "HELLO username truncated" if data.bytesize < p_offset + 1

          username = data.byteslice(1, u_len)
          p_len    = data.getbyte(p_offset)

          raise Error, "HELLO password truncated" if data.bytesize < p_offset + 1 + p_len

          password = data.byteslice(p_offset + 1, p_len)

          [username, password]
        end


        def extract_peer_info(cmd)
          props            = cmd.properties
          peer_socket_type = props["Socket-Type"]

          raise Error, "peer command missing Socket-Type" unless peer_socket_type

          {
            peer_socket_type: peer_socket_type,
            peer_identity:    props["Identity"] || "",
            peer_properties:  props,
          }
        end


        def send_command(io, cmd)
          io.write(cmd.to_frame.to_wire)
          io.flush
        end


        def read_command(io)
          frame = Codec::Frame.read_from(io)
          raise Error, "expected command frame, got data frame" unless frame.command?

          Codec::Command.from_body(frame.body)
        end
      end
    end
  end
end
