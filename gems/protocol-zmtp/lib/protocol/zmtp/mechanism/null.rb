# frozen_string_literal: true

module Protocol
  module ZMTP
    # Security mechanisms for the ZMTP handshake (NULL, PLAIN, CURVE).
    module Mechanism
      # NULL security mechanism — no encryption, no authentication.
      #
      # Performs the ZMTP 3.1 greeting exchange and READY command handshake.
      #
      class Null
        MECHANISM_NAME = "NULL"


        # Performs the full NULL handshake over +io+.
        #
        # 1. Exchange 64-byte greetings
        # 2. Validate peer greeting (version, mechanism)
        # 3. Exchange READY commands (socket type + identity + any extras)
        #
        # @param io [#read_exactly, #write, #flush] transport IO
        # @param as_server [Boolean]
        # @param socket_type [String]
        # @param identity [String]
        # @param metadata [Hash{String => String}, nil] extra READY properties
        # @return [Hash] { peer_socket_type:, peer_identity:, peer_public_key:, peer_properties:, peer_major:, peer_minor: }
        # @raise [Error]
        def handshake!(io, as_server:, socket_type:, identity:, metadata: nil)
          io.write(Codec::Greeting.encode(mechanism: MECHANISM_NAME, as_server: false))
          io.flush

          peer_greeting = Codec::Greeting.read_from(io)

          unless peer_greeting[:mechanism] == MECHANISM_NAME
            raise Error, "unsupported mechanism: #{peer_greeting[:mechanism]}"
          end

          ready_cmd = Codec::Command.ready(
            socket_type: socket_type,
            identity:    identity,
            metadata:    metadata,
          )
          io.write(ready_cmd.to_frame.to_wire)
          io.flush

          frame = Codec::Frame.read_from(io)
          unless frame.command?
            raise Error, "expected command frame, got data frame"
          end

          peer_cmd = Codec::Command.from_body(frame.body)
          unless peer_cmd.name == "READY"
            raise Error, "expected READY command, got #{peer_cmd.name}"
          end

          props            = peer_cmd.properties
          peer_socket_type = props["Socket-Type"]
          peer_identity    = props["Identity"] || ""

          unless peer_socket_type
            raise Error, "peer READY missing Socket-Type"
          end

          {
            peer_socket_type: peer_socket_type,
            peer_identity:    peer_identity,
            peer_public_key:  nil,
            peer_properties:  props,
            peer_major:       peer_greeting[:major],
            peer_minor:       peer_greeting[:minor],
          }
        end


        # @return [Boolean] false — NULL does not encrypt frames
        def encrypted? = false
      end
    end
  end
end
