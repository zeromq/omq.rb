# frozen_string_literal: true

module Protocol
  module ZMTP
    module Codec
      # ZMTP command encode/decode.
      #
      # Command frame body format:
      #   1 byte:    command name length
      #   N bytes:   command name
      #   remaining: command data
      #
      # READY command data = property list:
      #   1 byte:  property name length
      #   N bytes: property name
      #   4 bytes: property value length (big-endian)
      #   N bytes: property value
      #
      class Command
        # @return [String] command name (e.g. "READY", "SUBSCRIBE")
        attr_reader :name


        # @return [String] command data (binary)
        attr_reader :data


        # @param name [String] command name
        # @param data [String] command data
        def initialize(name, data = EMPTY_BINARY)
          @name = name
          @data = data.encoding == Encoding::BINARY ? data : data.b
        end


        # Encodes as a command frame body.
        #
        # @return [String] binary body (name-length + name + data)
        def to_body
          name_bytes = @name.encoding == Encoding::BINARY ? @name : @name.b
          buf = String.new(capacity: 1 + name_bytes.bytesize + @data.bytesize, encoding: Encoding::BINARY)
          buf << Frame::FLAG_BYTES[name_bytes.bytesize] << name_bytes << @data
        end


        # Encodes as a complete command Frame.
        #
        # @return [Frame]
        def to_frame
          Frame.new(to_body, command: true)
        end


        # Decodes a command from a frame body.
        #
        # @param body [String] binary frame body
        # @return [Command]
        # @raise [Error] on malformed command
        def self.from_body(body)
          body = body.b
          raise Error, "command body too short" if body.bytesize < 1

          name_len = body.getbyte(0)

          raise Error, "command name truncated" if body.bytesize < 1 + name_len

          name = body.byteslice(1, name_len)
          data = body.byteslice(1 + name_len..)
          new(name, data)
        end


        # Builds a READY command with Socket-Type, Identity, and any
        # extra properties supplied by upper layers (e.g. an extension
        # that injects +X-QoS+ or +X-Compression+).
        #
        # @param metadata [Hash{String => String}, nil] additional READY properties
        # @return [Command]
        def self.ready(socket_type:, identity: "", metadata: nil)
          props = { "Socket-Type" => socket_type, "Identity" => identity }
          props.merge!(metadata) if metadata && !metadata.empty?
          new("READY", encode_properties(props))
        end


        # Builds a SUBSCRIBE command.
        #
        # @param prefix [String] subscription prefix to match
        # @return [Command]
        def self.subscribe(prefix)
          new("SUBSCRIBE", prefix.b)
        end


        # Builds a CANCEL command (unsubscribe).
        #
        # @param prefix [String] subscription prefix to cancel
        # @return [Command]
        def self.cancel(prefix)
          new("CANCEL", prefix.b)
        end


        # Builds a JOIN command (RADIO/DISH group subscription).
        #
        # @param group [String] group name to join
        # @return [Command]
        def self.join(group)
          new("JOIN", group.b)
        end


        # Builds a LEAVE command (RADIO/DISH group unsubscription).
        #
        # @param group [String] group name to leave
        # @return [Command]
        def self.leave(group)
          new("LEAVE", group.b)
        end


        # Builds a PING command.
        #
        # @param ttl [Numeric] time-to-live in seconds (sent as deciseconds)
        # @param context [String] optional context bytes (up to 16 bytes)
        # @return [Command]
        def self.ping(ttl: 0, context: EMPTY_BINARY)
          ttl_ds = (ttl * 10).to_i
          new("PING", [ttl_ds].pack("n") + (context.encoding == Encoding::BINARY ? context : context.b))
        end


        # Builds a PONG command.
        #
        # @param context [String] context bytes echoed from the PING
        # @return [Command]
        def self.pong(context: EMPTY_BINARY)
          new("PONG", context.encoding == Encoding::BINARY ? context : context.b)
        end


        # Extracts TTL (in seconds) and context from a PING command's data.
        #
        # @return [Array(Numeric, String)] [ttl_seconds, context_bytes]
        def ping_ttl_and_context
          ttl_ds  = @data.unpack1("n")
          context = @data.bytesize > 2 ? @data.byteslice(2..) : EMPTY_BINARY
          [ttl_ds / 10.0, context]
        end


        # Parses READY command data as a property list.
        #
        # @return [Hash{String => String}] property name-value pairs
        def properties
          self.class.decode_properties(@data)
        end


        # Encodes a hash of properties into ZMTP property list format.
        #
        # @param props [Hash{String => String}] property name-value pairs
        # @return [String] binary-encoded property list
        def self.encode_properties(props)
          parts = props.map do |name, value|
            name_bytes  = name.b
            value_bytes = value.b
            name_bytes.bytesize.chr.b + name_bytes + [value_bytes.bytesize].pack("N") + value_bytes
          end
          parts.join
        end


        # Decodes a ZMTP property list from binary data.
        #
        # @param data [String] binary-encoded property list
        # @return [Hash{String => String}] property name-value pairs
        # @raise [Error] on malformed property data
        def self.decode_properties(data)
          result = {}
          offset = 0

          while offset < data.bytesize
            raise Error, "property name truncated" if offset + 1 > data.bytesize
            name_len = data.getbyte(offset)
            offset += 1

            raise Error, "property name truncated" if offset + name_len > data.bytesize
            name = data.byteslice(offset, name_len)
            offset += name_len

            raise Error, "property value length truncated" if offset + 4 > data.bytesize
            value_len = data.byteslice(offset, 4).unpack1("N")
            offset += 4

            raise Error, "property value truncated" if offset + value_len > data.bytesize
            value = data.byteslice(offset, value_len)
            offset += value_len

            result[name] = value
          end

          result
        end
      end
    end
  end
end
