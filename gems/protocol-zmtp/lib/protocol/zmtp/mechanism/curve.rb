# frozen_string_literal: true

module Protocol
  module ZMTP
    module Mechanism
      # CurveZMQ security mechanism (RFC 26).
      #
      # Provides Curve25519-XSalsa20-Poly1305 encryption and authentication
      # for ZMTP 3.1 connections.
      #
      # Crypto-backend-agnostic: pass any module that provides the NaCl API
      # (RbNaCl or Nuckle) via the +crypto:+ parameter.
      #
      # The crypto backend must provide:
      #   backend::PrivateKey.new(bytes) / .generate
      #   backend::PublicKey.new(bytes)
      #   backend::Box.new(peer_pub, my_secret)  → #encrypt(nonce, pt) / #decrypt(nonce, ct)
      #   backend::SecretBox.new(key)             → #encrypt(nonce, pt) / #decrypt(nonce, ct)
      #   backend::Random.random_bytes(n)
      #   backend::Util.verify32(a, b) / .verify64(a, b)
      #   backend::CryptoError (exception class)
      #
      class Curve
        MECHANISM_NAME = "CURVE"


        # Nonce prefixes.
        NONCE_PREFIX_HELLO     = "CurveZMQHELLO---"
        NONCE_PREFIX_WELCOME   = "WELCOME-"
        NONCE_PREFIX_INITIATE  = "CurveZMQINITIATE"
        NONCE_PREFIX_READY     = "CurveZMQREADY---"
        NONCE_PREFIX_MESSAGE_C = "CurveZMQMESSAGEC"
        NONCE_PREFIX_MESSAGE_S = "CurveZMQMESSAGES"
        NONCE_PREFIX_VOUCH     = "VOUCH---"
        NONCE_PREFIX_COOKIE    = "COOKIE--"


        BOX_OVERHEAD = 16
        MAX_NONCE    = (2**64) - 1


        # Creates a CURVE server mechanism.
        #
        # @param public_key [String] 32 bytes
        # @param secret_key [String] 32 bytes
        # @param crypto [Module] NaCl-compatible backend (RbNaCl or Nuckle)
        # @param authenticator [#call, nil] called with a {PeerInfo}
        #   during authentication; must return truthy to allow the connection.
        #   When nil, any client with a valid vouch is accepted.
        # @return [Curve]
        def self.server(public_key:, secret_key:, crypto:, authenticator: nil)
          new(public_key:, secret_key:, crypto:, as_server: true, authenticator:)
        end


        # Creates a CURVE client mechanism.
        #
        # @param server_key [String] 32 bytes (server permanent public key)
        # @param crypto [Module] NaCl-compatible backend (RbNaCl or Nuckle)
        # @param public_key [String, nil] 32 bytes (or nil for auto-generated ephemeral identity)
        # @param secret_key [String, nil] 32 bytes (or nil for auto-generated ephemeral identity)
        # @return [Curve]
        def self.client(server_key:, crypto:, public_key: nil, secret_key: nil)
          new(public_key:, secret_key:, server_key:, crypto:, as_server: false)
        end


        # @param public_key [String, nil] 32-byte permanent public key
        # @param secret_key [String, nil] 32-byte permanent secret key
        # @param server_key [String, nil] 32-byte server permanent public key (client only)
        # @param crypto [Module] NaCl-compatible crypto backend
        # @param as_server [Boolean] whether this side acts as the CURVE server
        # @param authenticator [#call, nil] optional server-side authenticator
        def initialize(public_key: nil, secret_key: nil, server_key: nil, crypto:, as_server: false, authenticator: nil)
          @crypto        = crypto
          @as_server     = as_server
          @authenticator = authenticator

          if as_server
            validate_key!(public_key, "public_key")
            validate_key!(secret_key, "secret_key")
            @permanent_public = crypto::PublicKey.new(public_key.b)
            @permanent_secret = crypto::PrivateKey.new(secret_key.b)
            @cookie_key = crypto::Random.random_bytes(32)
          else
            validate_key!(server_key, "server_key")
            @server_public = crypto::PublicKey.new(server_key.b)
            if public_key && secret_key
              validate_key!(public_key, "public_key")
              validate_key!(secret_key, "secret_key")
              @permanent_public = crypto::PublicKey.new(public_key.b)
              @permanent_secret = crypto::PrivateKey.new(secret_key.b)
            else
              @permanent_secret = crypto::PrivateKey.generate
              @permanent_public = @permanent_secret.public_key
            end
          end

          @session_box = nil
          @send_nonce  = 0
          @recv_nonce  = -1
        end


        # Resets session state when duplicating (e.g. for a new connection).
        #
        # @param source [Curve] the original instance being duplicated
        # @return [void]
        def initialize_dup(source)
          super
          @session_box    = nil
          @send_nonce     = 0
          @recv_nonce     = -1
          @send_nonce_buf = nil
          @recv_nonce_buf = nil
        end


        # @return [Boolean] true -- CURVE always encrypts frames
        def encrypted? = true

        # Returns a periodic maintenance task for rotating the cookie key (server only).
        #
        # @return [Hash, nil] a hash with +:interval+ (seconds) and +:task+ (Proc), or nil for clients
        def maintenance
          return unless @as_server
          { interval: 60, task: -> { @cookie_key = @crypto::Random.random_bytes(32) } }.freeze
        end


        # Performs the full CurveZMQ handshake (HELLO/WELCOME/INITIATE/READY).
        #
        # @param io [#read_exactly, #write, #flush] transport IO
        # @param as_server [Boolean] ignored -- uses the value from #initialize
        # @param socket_type [String] our socket type name
        # @param identity [String] our identity
        # @param metadata [Hash{String => String}, nil] extra READY properties
        # @return [Hash] { peer_socket_type:, peer_identity:, peer_properties: }
        # @raise [Error] on handshake failure
        def handshake!(io, as_server:, socket_type:, identity:, metadata: nil)
          if @as_server
            server_handshake!(io, socket_type:, identity:, metadata:)
          else
            client_handshake!(io, socket_type:, identity:, metadata:)
          end
        end


        # Encrypts a frame body into a CURVE MESSAGE command on the wire.
        #
        # @param body [String] plaintext frame body
        # @param more [Boolean] whether more frames follow in this message
        # @param command [Boolean] whether this is a command frame
        # @return [String] binary wire bytes ready for writing
        def encrypt(body, more: false, command: false)
          flags = 0
          flags |= 0x01 if more
          flags |= 0x02 if command

          plaintext = String.new(encoding: Encoding::BINARY, capacity: 1 + body.bytesize)
          plaintext << flags << body

          nonce       = make_send_nonce
          ciphertext  = @session_box.encrypt(nonce, plaintext)
          short_nonce = nonce.byteslice(16, 8)

          msg_body_size = 16 + ciphertext.bytesize
          if msg_body_size > 255
            wire = String.new(encoding: Encoding::BINARY, capacity: 9 + msg_body_size)
            wire << "\x02" << [msg_body_size].pack("Q>")
          else
            wire = String.new(encoding: Encoding::BINARY, capacity: 2 + msg_body_size)
            wire << "\x00" << msg_body_size
          end
          wire << "\x07MESSAGE" << short_nonce << ciphertext
        end


        MESSAGE_PREFIX      = "\x07MESSAGE".b.freeze
        MESSAGE_PREFIX_SIZE = MESSAGE_PREFIX.bytesize


        # Decrypts a CURVE MESSAGE command frame back into a plaintext frame.
        #
        # @param frame [Codec::Frame] an encrypted MESSAGE command frame
        # @return [Codec::Frame] the decrypted frame with restored flags
        # @raise [Error] on decryption failure or nonce violation
        def decrypt(frame)
          body = frame.body
          unless body.start_with?(MESSAGE_PREFIX)
            raise Error, "expected MESSAGE command"
          end

          data = body.byteslice(MESSAGE_PREFIX_SIZE..)
          raise Error, "MESSAGE too short" if data.bytesize < 8 + BOX_OVERHEAD

          short_nonce = data.byteslice(0, 8)
          ciphertext  = data.byteslice(8..)

          nonce_value = short_nonce.unpack1("Q>")
          unless nonce_value > @recv_nonce
            raise Error, "MESSAGE nonce not strictly incrementing"
          end
          @recv_nonce = nonce_value

          @recv_nonce_buf[16, 8] = short_nonce
          begin
            plaintext = @session_box.decrypt(@recv_nonce_buf, ciphertext)
          rescue @crypto::CryptoError
            raise Error, "MESSAGE decryption failed"
          end

          flags = plaintext.getbyte(0)
          body  = plaintext.byteslice(1..) || "".b
          Codec::Frame.new(body, more: (flags & 0x01) != 0, command: (flags & 0x02) != 0)
        end

        private

        # ----------------------------------------------------------------
        # Client-side handshake
        # ----------------------------------------------------------------

        def client_handshake!(io, socket_type:, identity:, metadata: nil)
          cn_secret = @crypto::PrivateKey.generate
          cn_public = cn_secret.public_key

          io.write(Codec::Greeting.encode(mechanism: MECHANISM_NAME, as_server: false))
          io.flush
          peer_greeting = Codec::Greeting.read_from(io)
          unless peer_greeting[:mechanism] == MECHANISM_NAME
            raise Error, "expected CURVE mechanism, got #{peer_greeting[:mechanism]}"
          end
          @peer_major = peer_greeting[:major]
          @peer_minor = peer_greeting[:minor]


          # --- HELLO ---
          short_nonce = [1].pack("Q>")
          nonce       = NONCE_PREFIX_HELLO + short_nonce
          hello_box   = @crypto::Box.new(@server_public, cn_secret)
          signature   = hello_box.encrypt(nonce, "\x00" * 64)

          hello = "".b
          hello << "\x05HELLO"
          hello << "\x01\x00"
          hello << ("\x00" * 72)
          hello << cn_public.to_s
          hello << short_nonce
          hello << signature

          io.write(Codec::Frame.new(hello, command: true).to_wire)
          io.flush

          # --- Read WELCOME ---
          welcome_frame = Codec::Frame.read_from(io)
          raise Error, "expected command frame" unless welcome_frame.command?
          welcome_cmd = Codec::Command.from_body(welcome_frame.body)
          raise Error, "expected WELCOME, got #{welcome_cmd.name}" unless welcome_cmd.name == "WELCOME"

          wdata = welcome_cmd.data
          raise Error, "WELCOME wrong size" unless wdata.bytesize == 16 + 144

          w_short_nonce = wdata.byteslice(0, 16)
          w_box_data    = wdata.byteslice(16, 144)
          w_nonce       = NONCE_PREFIX_WELCOME + w_short_nonce

          begin
            w_plaintext = @crypto::Box.new(@server_public, cn_secret).decrypt(w_nonce, w_box_data)
          rescue @crypto::CryptoError
            raise Error, "WELCOME decryption failed"
          end

          sn_public = @crypto::PublicKey.new(w_plaintext.byteslice(0, 32))
          cookie    = w_plaintext.byteslice(32, 96)

          session = @crypto::Box.new(sn_public, cn_secret)

          # --- INITIATE ---
          vouch_nonce     = NONCE_PREFIX_VOUCH + @crypto::Random.random_bytes(16)
          vouch_plaintext = cn_public.to_s + @server_public.to_s
          vouch           = @crypto::Box.new(sn_public, @permanent_secret).encrypt(vouch_nonce, vouch_plaintext)

          props = { "Socket-Type" => socket_type, "Identity" => identity }
          props.merge!(metadata) if metadata && !metadata.empty?
          metadata_bytes = Codec::Command.encode_properties(props)

          initiate_box_plaintext = "".b
          initiate_box_plaintext << @permanent_public.to_s
          initiate_box_plaintext << vouch_nonce.byteslice(8, 16)
          initiate_box_plaintext << vouch
          initiate_box_plaintext << metadata_bytes

          init_short_nonce = [1].pack("Q>")
          init_nonce       = NONCE_PREFIX_INITIATE + init_short_nonce
          init_ciphertext  = session.encrypt(init_nonce, initiate_box_plaintext)

          initiate = "".b
          initiate << "\x08INITIATE"
          initiate << cookie
          initiate << init_short_nonce
          initiate << init_ciphertext

          io.write(Codec::Frame.new(initiate, command: true).to_wire)
          io.flush

          # --- Read READY ---
          ready_frame = Codec::Frame.read_from(io)
          raise Error, "expected command frame" unless ready_frame.command?
          ready_cmd = Codec::Command.from_body(ready_frame.body)
          raise Error, "expected READY, got #{ready_cmd.name}" unless ready_cmd.name == "READY"

          rdata = ready_cmd.data
          raise Error, "READY too short" if rdata.bytesize < 8 + BOX_OVERHEAD

          r_short_nonce = rdata.byteslice(0, 8)
          r_ciphertext  = rdata.byteslice(8..)
          r_nonce       = NONCE_PREFIX_READY + r_short_nonce

          begin
            r_plaintext = session.decrypt(r_nonce, r_ciphertext)
          rescue @crypto::CryptoError
            raise Error, "READY decryption failed"
          end

          props            = Codec::Command.decode_properties(r_plaintext)
          peer_socket_type = props["Socket-Type"]
          peer_identity    = props["Identity"] || ""

          @session_box = session
          @send_nonce  = 1
          @recv_nonce  = 0
          init_nonce_buffers!

          {
            peer_socket_type: peer_socket_type,
            peer_identity:    peer_identity,
            peer_public_key:  @server_public,
            peer_properties:  props,
            peer_major:       @peer_major,
            peer_minor:       @peer_minor,
          }
        end


        # ----------------------------------------------------------------
        # Server-side handshake
        # ----------------------------------------------------------------

        def server_handshake!(io, socket_type:, identity:, metadata: nil)
          io.write(Codec::Greeting.encode(mechanism: MECHANISM_NAME, as_server: true))
          io.flush
          peer_greeting = Codec::Greeting.read_from(io)
          unless peer_greeting[:mechanism] == MECHANISM_NAME
            raise Error, "expected CURVE mechanism, got #{peer_greeting[:mechanism]}"
          end
          @peer_major = peer_greeting[:major]
          @peer_minor = peer_greeting[:minor]


          # --- Read HELLO ---
          hello_frame = Codec::Frame.read_from(io)
          raise Error, "expected command frame" unless hello_frame.command?
          hello_cmd = Codec::Command.from_body(hello_frame.body)
          raise Error, "expected HELLO, got #{hello_cmd.name}" unless hello_cmd.name == "HELLO"

          hdata = hello_cmd.data
          raise Error, "HELLO wrong size (#{hdata.bytesize})" unless hdata.bytesize == 194

          cn_public     = @crypto::PublicKey.new(hdata.byteslice(74, 32))
          h_short_nonce = hdata.byteslice(106, 8)
          h_signature   = hdata.byteslice(114, 80)

          h_nonce = NONCE_PREFIX_HELLO + h_short_nonce
          begin
            plaintext = @crypto::Box.new(cn_public, @permanent_secret).decrypt(h_nonce, h_signature)
          rescue @crypto::CryptoError
            raise Error, "HELLO signature verification failed"
          end
          unless @crypto::Util.verify64(plaintext, "\x00" * 64)
            raise Error, "HELLO signature content invalid"
          end


          # --- WELCOME ---
          sn_secret = @crypto::PrivateKey.generate
          sn_public = sn_secret.public_key

          cookie_nonce     = NONCE_PREFIX_COOKIE + @crypto::Random.random_bytes(16)
          cookie_plaintext = cn_public.to_s + sn_secret.to_s
          cookie           = cookie_nonce.byteslice(8, 16) +
                             @crypto::SecretBox.new(@cookie_key).encrypt(cookie_nonce, cookie_plaintext)

          w_plaintext   = sn_public.to_s + cookie
          w_short_nonce = @crypto::Random.random_bytes(16)
          w_nonce       = NONCE_PREFIX_WELCOME + w_short_nonce
          w_ciphertext  = @crypto::Box.new(cn_public, @permanent_secret).encrypt(w_nonce, w_plaintext)

          welcome = "".b
          welcome << "\x07WELCOME"
          welcome << w_short_nonce
          welcome << w_ciphertext

          io.write(Codec::Frame.new(welcome, command: true).to_wire)
          io.flush

          # --- Read INITIATE ---
          init_frame = Codec::Frame.read_from(io)
          raise Error, "expected command frame" unless init_frame.command?
          init_cmd = Codec::Command.from_body(init_frame.body)
          raise Error, "expected INITIATE, got #{init_cmd.name}" unless init_cmd.name == "INITIATE"

          idata = init_cmd.data
          raise Error, "INITIATE too short" if idata.bytesize < 96 + 8 + BOX_OVERHEAD

          recv_cookie   = idata.byteslice(0, 96)
          i_short_nonce = idata.byteslice(96, 8)
          i_ciphertext  = idata.byteslice(104..)

          cookie_short_nonce   = recv_cookie.byteslice(0, 16)
          cookie_ciphertext    = recv_cookie.byteslice(16, 80)
          cookie_decrypt_nonce = NONCE_PREFIX_COOKIE + cookie_short_nonce
          begin
            cookie_contents = @crypto::SecretBox.new(@cookie_key).decrypt(cookie_decrypt_nonce, cookie_ciphertext)
          rescue @crypto::CryptoError
            raise Error, "INITIATE cookie verification failed"
          end

          cn_public = @crypto::PublicKey.new(cookie_contents.byteslice(0, 32))
          sn_secret = @crypto::PrivateKey.new(cookie_contents.byteslice(32, 32))

          session = @crypto::Box.new(cn_public, sn_secret)
          i_nonce = NONCE_PREFIX_INITIATE + i_short_nonce

          begin
            i_plaintext = session.decrypt(i_nonce, i_ciphertext)
          rescue @crypto::CryptoError
            raise Error, "INITIATE decryption failed"
          end

          raise Error, "INITIATE plaintext too short" if i_plaintext.bytesize < 32 + 16 + 80

          client_permanent  = @crypto::PublicKey.new(i_plaintext.byteslice(0, 32))
          vouch_short_nonce = i_plaintext.byteslice(32, 16)
          vouch_ciphertext  = i_plaintext.byteslice(48, 80)
          metadata_bytes    = i_plaintext.byteslice(128..) || "".b

          vouch_nonce = NONCE_PREFIX_VOUCH + vouch_short_nonce
          begin
            vouch_plaintext = @crypto::Box.new(client_permanent, sn_secret).decrypt(vouch_nonce, vouch_ciphertext)
          rescue @crypto::CryptoError
            raise Error, "INITIATE vouch verification failed"
          end

          raise Error, "vouch wrong size" unless vouch_plaintext.bytesize == 64

          vouch_cn     = vouch_plaintext.byteslice(0, 32)
          vouch_server = vouch_plaintext.byteslice(32, 32)

          unless @crypto::Util.verify32(vouch_cn, cn_public.to_s)
            raise Error, "vouch client transient key mismatch"
          end
          unless @crypto::Util.verify32(vouch_server, @permanent_public.to_s)
            raise Error, "vouch server key mismatch"
          end

          if @authenticator
            peer = PeerInfo.new(public_key: client_permanent, identity: "")
            unless @authenticator.call(peer)
              send_error(io, "client key not authorized")
              raise Error, "client key not authorized"
            end
          end


          # --- READY ---
          ready_props = { "Socket-Type" => socket_type, "Identity" => identity }
          ready_props.merge!(metadata) if metadata && !metadata.empty?
          ready_metadata = Codec::Command.encode_properties(ready_props)

          r_short_nonce = [1].pack("Q>")
          r_nonce       = NONCE_PREFIX_READY + r_short_nonce
          r_ciphertext  = session.encrypt(r_nonce, ready_metadata)

          ready = "".b
          ready << "\x05READY"
          ready << r_short_nonce
          ready << r_ciphertext

          io.write(Codec::Frame.new(ready, command: true).to_wire)
          io.flush

          props = Codec::Command.decode_properties(metadata_bytes)

          @session_box = session
          @send_nonce  = 1
          @recv_nonce  = 0
          init_nonce_buffers!

          {
            peer_socket_type: props["Socket-Type"],
            peer_identity:    props["Identity"] || "",
            peer_public_key:  client_permanent,
            peer_properties:  props,
            peer_major:       @peer_major,
            peer_minor:       @peer_minor,
          }
        end


        # ----------------------------------------------------------------
        # Nonce helpers
        # ----------------------------------------------------------------

        def init_nonce_buffers!
          send_pfx = @as_server ? NONCE_PREFIX_MESSAGE_S : NONCE_PREFIX_MESSAGE_C
          recv_pfx = @as_server ? NONCE_PREFIX_MESSAGE_C : NONCE_PREFIX_MESSAGE_S
          @send_nonce_buf = String.new(send_pfx + ("\x00" * 8), encoding: Encoding::BINARY)
          @recv_nonce_buf = String.new(recv_pfx + ("\x00" * 8), encoding: Encoding::BINARY)
        end


        def make_send_nonce
          @send_nonce += 1
          raise Error, "nonce counter exhausted" if @send_nonce > MAX_NONCE
          n = @send_nonce
          @send_nonce_buf.setbyte(23, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(22, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(21, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(20, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(19, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(18, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(17, n & 0xFF)
          n >>= 8
          @send_nonce_buf.setbyte(16, n & 0xFF)
          @send_nonce_buf
        end


        def send_error(io, reason)
          error_body = "".b
          error_body << "\x05ERROR"
          error_body << reason.bytesize.chr << reason.b
          io.write(Codec::Frame.new(error_body, command: true).to_wire)
          io.flush
        rescue IOError
          # connection may already be broken
        end


        def validate_key!(key, name)
          raise ArgumentError, "#{name} is required" if key.nil?
          raise ArgumentError, "#{name} must be 32 bytes (got #{key.b.bytesize})" unless key.b.bytesize == 32
        end
      end
    end
  end
end
