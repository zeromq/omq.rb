# frozen_string_literal: true

module Protocol
  module ZMTP
    # Manages one ZMTP peer connection over any transport IO.
    #
    # Delegates the security handshake to a Mechanism object (Null, Curve, etc.),
    # then provides message send/receive and command send/receive on top of the
    # framing codec.
    #
    # Heartbeat timing is tracked but not driven — the caller (e.g. an engine)
    # is responsible for periodically sending PINGs and checking expiry.
    #
    class Connection
      # @return [String] peer's socket type (from READY handshake)
      attr_reader :peer_socket_type


      # @return [String] peer's identity (from READY handshake)
      attr_reader :peer_identity


      # @return [Object, nil] peer's CURVE long-term public key post-handshake
      #   (+crypto::PublicKey+ when the mechanism is CURVE; +nil+ for NULL).
      attr_reader :peer_public_key


      # @return [Hash{String => String}, nil] full peer READY property hash
      #   (set after a successful handshake; nil before).
      #   Upper layers extract their own X-* properties from here.
      attr_reader :peer_properties


      # @return [Integer, nil] peer ZMTP major version (from greeting)
      attr_reader :peer_major


      # @return [Integer, nil] peer ZMTP minor version (from greeting);
      #   0 for ZMTP 3.0 peers, 1 for ZMTP 3.1+
      attr_reader :peer_minor


      # @return [Object] transport IO (#peek, #read_exactly, #write, #flush, #close)
      attr_reader :io


      # @return [Float, nil] monotonic timestamp of last received frame
      attr_reader :last_received_at


      # @param io [#peek, #read_exactly, #write, #flush, #close] transport IO
      # @param socket_type [String] our socket type name (e.g. "REQ")
      # @param identity [String] our identity
      # @param as_server [Boolean] whether we are the server side
      # @param mechanism [Mechanism::Null, Mechanism::Curve] security mechanism
      # @param max_message_size [Integer, nil] max frame size in bytes, nil = unlimited
      # @param opts [Hash{String => String}] extra READY properties to
      #   advertise (e.g. +"X-QoS" => "1"+). Upper-layer extensions use
      #   this to inject their own negotiated properties without the
      #   codec needing to know about them.
      def initialize(io, socket_type:, identity: "", as_server: false,
                     mechanism: nil, max_message_size: nil, **opts)
        @io               = io
        @socket_type      = socket_type
        @identity         = identity
        @as_server        = as_server
        @mechanism        = mechanism || Mechanism::Null.new
        @peer_socket_type = nil
        @peer_identity    = nil
        @peer_public_key  = nil
        @peer_properties  = nil
        @peer_major       = nil
        @peer_minor       = nil
        @metadata         = opts.empty? ? nil : opts.transform_keys(&:to_s)
        @mutex            = Mutex.new
        @max_message_size = max_message_size
        @last_received_at = nil

        # Reusable scratch buffer for frame headers. Array#pack(buffer:)
        # writes in place so the per-message 2-or-9 byte String allocation
        # in write_frames disappears on the hot send path.
        @header_buf = String.new(capacity: 9, encoding: Encoding::BINARY)
        @frame_buf  = String.new(capacity: 257, encoding: Encoding::BINARY)
      end


      # Performs the full ZMTP handshake via the configured mechanism.
      #
      # @return [void]
      # @raise [Error] on handshake failure
      def handshake!
        result = @mechanism.handshake! @io,
          as_server:   @as_server,
          socket_type: @socket_type,
          identity:    @identity,
          metadata:    @metadata

        @peer_socket_type = result[:peer_socket_type]
        @peer_identity    = result[:peer_identity]
        @peer_public_key  = result[:peer_public_key]
        @peer_properties  = result[:peer_properties]
        @peer_major       = result[:peer_major]
        @peer_minor       = result[:peer_minor]

        unless @peer_socket_type
          raise Error, "peer READY missing Socket-Type"
        end

        unless VALID_PEERS[@socket_type.to_sym]&.include?(@peer_socket_type.to_sym)
          raise Error,
                "incompatible socket types: #{@socket_type} cannot connect to #{@peer_socket_type}"
        end
      end


      # Returns a {PeerInfo} value bundling the peer's CURVE public key
      # and identity for use as a stable per-peer key (frozen, hash-usable).
      # Nil before the handshake has completed.
      #
      # @return [PeerInfo, nil]
      def peer_info
        return nil unless @peer_socket_type
        PeerInfo.new(public_key: @peer_public_key, identity: @peer_identity)
      end


      # Sends a multi-frame message (write + flush).
      #
      # @param parts [Array<String>] message frames
      # @return [void]
      def send_message(parts)
        with_deferred_cancel do
          @mutex.synchronize do
            write_frames(parts)
            @io.flush
          end
        end
      end


      # Writes a multi-frame message to the buffer without flushing.
      # Call {#flush} after batching writes.
      #
      # @param parts [Array<String>] message frames
      # @return [void]
      def write_message(parts)
        with_deferred_cancel do
          @mutex.synchronize do
            write_frames(parts)
          end
        end
      end


      # Writes a batch of multi-frame messages to the buffer under a
      # single mutex acquisition. Used by work-stealing send pumps that
      # dequeue up to N messages at once — avoids the N lock/unlock
      # pairs per batch that a plain `batch.each { write_message }`
      # would incur.
      #
      # @param messages [Array<Array<String>>] each element is one
      #   multi-frame message
      # @return [void]
      def write_messages(messages)
        with_deferred_cancel do
          @mutex.synchronize do
            i = 0
            n = messages.size
            while i < n
              write_frames(messages[i])
              i += 1
            end
          end
        end
      end


      # Writes pre-encoded wire bytes to the buffer without flushing.
      # Used for fan-out: encode once, write to many connections.
      #
      # @param wire_bytes [String] ZMTP wire-format bytes
      # @return [void]
      def write_wire(wire_bytes)
        with_deferred_cancel do
          @mutex.synchronize do
            @io.write(wire_bytes)
          end
        end
      end


      # Writes multiple pre-encoded wire byte strings under a single
      # mutex acquisition.
      #
      # @param wire_strings [Array<String>]
      # @return [void]
      def write_wire_batch(wire_strings)
        with_deferred_cancel do
          @mutex.synchronize do
            i = 0
            n = wire_strings.size
            while i < n
              @io.write(wire_strings[i])
              i += 1
            end
          end
        end
      end


      # Returns true if the ZMTP mechanism encrypts at the frame level
      # (e.g. CURVE, BLAKE3ZMQ).
      #
      # @return [Boolean]
      def encrypted?
        @mechanism.encrypted?
      end


      # Flushes the write buffer to the underlying IO.
      #
      # @return [void]
      def flush
        @mutex.synchronize do
          @io.flush
        end
      end


      # Receives a multi-frame message.
      # PING/PONG commands are handled automatically by #read_frame.
      #
      # @return [Array<String>] message frames
      # @raise [EOFError] if connection is closed
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


      # Sends a command.
      #
      # @param command [Codec::Command]
      # @return [void]
      def send_command(command)
        with_deferred_cancel do
          @mutex.synchronize do
            if @mechanism.encrypted?
              @io.write(@mechanism.encrypt(command.to_body, command: true))
            else
              @io.write(command.to_frame.to_wire)
            end
            @io.flush
          end
        end
      end


      # Reads one frame from the wire. Handles PING/PONG automatically.
      # When using an encrypted mechanism, all frames are decrypted
      # transparently (supports both CURVE MESSAGE wrapping and inline
      # encryption like BLAKE3ZMQ).
      #
      # @return [Codec::Frame]
      # @raise [EOFError] if connection is closed
      def read_frame
        loop do
          begin
            frame = Codec::Frame.read_from(@io, max_message_size: @max_message_size)
          rescue Error
            close
            raise
          end

          touch_heartbeat

          frame = @mechanism.decrypt(frame) if @mechanism.encrypted?

          if frame.command?
            cmd = Codec::Command.from_body(frame.body)
            case cmd.name
            when "PING"
              _, context = cmd.ping_ttl_and_context
              send_command(Codec::Command.pong(context: context))
              next
            when "PONG"
              next
            end
          end

          return frame
        end
      end


      # Records that a frame was received (for heartbeat expiry tracking).
      #
      # @return [void]
      def touch_heartbeat
        @last_received_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end


      # Returns true if no frame has been received within +timeout+ seconds.
      #
      # @param timeout [Numeric] seconds
      # @return [Boolean]
      def heartbeat_expired?(timeout)
        return false unless @last_received_at
        (Process.clock_gettime(Process::CLOCK_MONOTONIC) - @last_received_at) > timeout
      end


      # Closes the connection.
      #
      # @return [void]
      def close
        @io.close
      rescue IOError
        # already closed
      end

      private

      # Defers task cancellation around a block of wire writes so the
      # peer never sees a half-written frame or partial multipart
      # message. Without this, an +Async::Cancel+ arriving between
      # successive frames (or between header and body writes for long
      # frames) would desync the peer's framer unrecoverably.
      #
      # When called outside an Async task (test fixtures, blocking
      # callers), the block runs directly -- there is no task to defer
      # on. Cancellation arriving from inside the block (peer
      # disconnect raising +EPIPE+/+EOFError+) propagates normally.
      def with_deferred_cancel
        if defined?(Async::Task) && (task = Async::Task.current?)
          task.defer_cancel { yield }
        else
          yield
        end
      end


      # Writes message parts as ZMTP frames, encrypting if needed.
      #
      # Short frames (body <= 255 B) combine the 2-byte header and
      # body into a reusable buffer for a single +@io.write+, halving
      # the per-frame mutex overhead in io-stream. Long frames write
      # header and body separately to avoid copying the body.
      def write_frames(parts)
        encrypted  = @mechanism.encrypted?
        buf        = @header_buf
        fbuf       = @frame_buf
        flag_bytes = Codec::Frame::FLAG_BYTES
        last       = parts.size - 1

        i = 0

        while i < parts.size
          part = parts[i]
          more = i < last

          if encrypted
            body = part.encoding == Encoding::BINARY ? part : part.b
            @io.write(@mechanism.encrypt(body, more: more))
          else
            body  = part.encoding == Encoding::BINARY ? part : part.b
            size  = body.bytesize
            flags = more ? Codec::Frame::FLAGS_MORE : 0

            if size > Codec::Frame::SHORT_MAX
              buf.clear
              [flags | Codec::Frame::FLAGS_LONG, size].pack("CQ>", buffer: buf)
              @io.write(buf)
              @io.write(body)
            else
              fbuf.clear
              fbuf << flag_bytes[flags]
              fbuf << flag_bytes[size]
              fbuf << body
              @io.write(fbuf)
            end
          end

          i += 1
        end
      end

    end
  end
end
