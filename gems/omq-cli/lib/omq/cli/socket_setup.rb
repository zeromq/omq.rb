# frozen_string_literal: true

module OMQ
  module CLI
    # Stateless helper for socket construction and configuration.
    # All methods are module-level so callers compose rather than inherit.
    #
    module SocketSetup
      # Default high water mark applied when the user does not pass
      # --hwm. Lower than libzmq's default (1000) to keep memory
      # footprint bounded for the typical CLI use cases (interactive
      # debugging, short-lived pipelines). 64 matches the recv pump's
      # per-fairness-batch limit (one batch exactly fills a full
      # queue).
      DEFAULT_HWM = 64

      # Default max inbound message size applied when the user does not
      # pass --recv-maxsz. The omq library itself is unlimited by default;
      # the CLI caps inbound messages at 1 MiB so that a misconfigured or
      # malicious peer cannot force arbitrary memory allocation on a
      # terminal user. Users can raise it with --recv-maxsz N, or disable
      # it entirely with --recv-maxsz 0.
      DEFAULT_RECV_MAXSZ = 1 << 20

      # Default zstd level when the user picks zstd without specifying one
      # (e.g. `--compress=zstd`). Matches the `-z` shortcut.
      DEFAULT_ZSTD_LEVEL = -3

      # Resolves compression state for a given pipe +side+ (:in, :out, or nil).
      # Per-side settings (in_compress / out_compress) override the global
      # +config.compress+. Returns +[codec, level]+ where codec is one of
      # +nil+ (off), +:zstd+, or +:lz4+.
      #
      def self.resolve_compress(config, side)
        scoped =
          case side
          when :in  then !config.in_compress.nil?
          when :out then !config.out_compress.nil?
          end

        if scoped && side == :in
          [config.in_compress,  config.in_compress_level]
        elsif scoped && side == :out
          [config.out_compress, config.out_compress_level]
        else
          [config.compress, config.compress_level]
        end
      end


      # Upgrades a +tcp://+ URL to the codec-specific variant
      # (+zstd+tcp://+ or +lz4+tcp://+) when compression is enabled
      # for the given +side+. Returns the URL unchanged for non-TCP or
      # when compression is off.
      #
      def self.compress_url(url, config, side: nil)
        codec, _ = resolve_compress(config, side)
        return url if codec.nil?
        return url unless url.start_with?("tcp://")

        url.sub("tcp://", "#{codec}+tcp://")
      end


      # Returns bind/connect kwargs for the chosen codec on the given
      # +side+. Zstd accepts a +level:+; LZ4 takes no tuning knobs.
      #
      def self.compress_opts(config, side: nil)
        codec, level = resolve_compress(config, side)
        case codec
        when :zstd then { level: level || DEFAULT_ZSTD_LEVEL }
        else            {}
        end
      end


      # Human-readable CLI flag for a resolved compress state, suitable
      # for process titles and proctitle-like log lines.
      #
      def self.compress_flag(config, side: nil)
        codec, level = resolve_compress(config, side)
        case codec
        when :zstd then level == 3 ? "-Z" : "-z"
        when :lz4  then "--lz4"
        end
      end


      # Returns socket constructor kwargs for the selected backend.
      #
      def self.backend_kwargs(config)
        backend = backend_name(config)
        return {} if backend.nil? || backend == :ruby

        { backend: backend }
      end


      # Returns the effective backend name after legacy --ffi handling.
      #
      def self.backend_name(config)
        backend = if config.respond_to?(:ffi) && config.ffi
                    :libzmq
                  elsif config.respond_to?(:backend)
                    config.backend
                  else
                    :ruby
                  end
        backend = :libzmq if backend == :ffi
        backend || :ruby
      end


      # Apply common socket options from +config+ to +sock+.
      #
      def self.apply_options(sock, config)
        sock.linger             = config.linger
        sock.recv_timeout       = config.timeout       if config.timeout
        sock.send_timeout       = config.timeout       if config.timeout
        sock.reconnect_interval = config.reconnect_ivl if config.reconnect_ivl
        sock.heartbeat_interval = config.heartbeat_ivl if config.heartbeat_ivl
        # nil → default; 0 stays 0 (unbounded), any other integer is taken as-is.
        sock.send_hwm           = config.send_hwm || DEFAULT_HWM
        sock.recv_hwm           = config.recv_hwm || DEFAULT_HWM
        sock.sndbuf             = config.sndbuf        if config.sndbuf
        sock.rcvbuf             = config.rcvbuf        if config.rcvbuf
      end


      # Create and fully configure a socket from +klass+ and +config+.
      #
      def self.build(klass, config)
        sock = klass.new(**backend_kwargs(config))
        sock.conflate = true if config.conflate && %w[pub radio].include?(config.type_name)
        apply_options(sock, config)
        apply_recv_maxsz(sock, config)
        sock.identity         = config.identity   if config.identity
        sock.router_mandatory = true if config.type_name == "router"
        sock
      end


      # --recv-maxsz: nil → 1 MiB default; 0 → explicitly unlimited; else → as-is.
      def self.apply_recv_maxsz(sock, config)
        sock.max_message_size =
          case config.recv_maxsz
          when nil then DEFAULT_RECV_MAXSZ
          when 0   then nil
          else          config.recv_maxsz
          end
      end


      # Bind/connect +sock+ using URL strings from +config.binds+ / +config.connects+.
      # +verbose+ gates logging (>= 1), +timestamps+ controls prefix.
      #
      def self.attach(sock, config, verbose: 0, timestamps: nil)
        opts = compress_opts(config)

        config.binds.each do |url|
          uri = sock.bind(compress_url(url, config), **opts)
          CLI::Term.write_attach(:bind, uri.to_s, timestamps) if verbose >= 1
        end

        config.connects.each do |url|
          sock.connect(compress_url(url, config), **opts)
          CLI::Term.write_attach(:connect, url, timestamps) if verbose >= 1
        end
      end


      # Bind/connect +sock+ from an Array of Endpoint objects.
      # Used by PipeRunner, which works with structured endpoint lists.
      # +verbose+ gates logging (>= 1), +timestamps+ controls prefix.
      # +side+ selects per-socket compression (:in for PULL, :out for
      # PUSH in a pipe); nil falls back to global compress settings.
      #
      def self.attach_endpoints(sock, endpoints, config: nil, verbose: 0, timestamps: nil, side: nil)
        opts = config ? compress_opts(config, side: side) : {}

        endpoints.each do |ep|
          url = config ? compress_url(ep.url, config, side: side) : ep.url

          if ep.bind?
            uri = sock.bind(url, **opts)
            CLI::Term.write_attach(:bind, uri.to_s, timestamps) if verbose >= 1
          else
            sock.connect(url, **opts)
            CLI::Term.write_attach(:connect, ep.url, timestamps) if verbose >= 1
          end
        end
      end


      # Subscribe or join groups on +sock+ according to +config+.
      #
      def self.setup_subscriptions(sock, config)
        case config.type_name
        when "sub"
          prefixes = config.subscribes.empty? ? [""] : config.subscribes
          prefixes.each { |p| sock.subscribe(p) }
        when "dish"
          config.joins.each { |g| sock.join(g) }
        end
      end


      # Configure CURVE encryption on +sock+ using +config+ and env vars.
      #
      def self.setup_curve(sock, config)
        server_key_z85 = config.curve_server_key || ENV["OMQ_SERVER_KEY"]
        server_mode    = config.curve_server || (ENV["OMQ_SERVER_PUBLIC"] && ENV["OMQ_SERVER_SECRET"])

        return unless server_key_z85 || server_mode

        crypto = CLI.load_curve_crypto(config.crypto || ENV["OMQ_CRYPTO"], verbose: config.verbose >= 1)
        require "protocol/zmtp/mechanism/curve"

        if server_key_z85
          server_key = Protocol::ZMTP::Z85.decode(server_key_z85)
          client_key = crypto::PrivateKey.generate
          sock.mechanism = Protocol::ZMTP::Mechanism::Curve.client(
            public_key: client_key.public_key.to_s,
            secret_key: client_key.to_s,
            server_key: server_key,
            crypto: crypto
          )
        elsif server_mode
          if ENV["OMQ_SERVER_PUBLIC"] && ENV["OMQ_SERVER_SECRET"]
            server_pub = Protocol::ZMTP::Z85.decode(ENV["OMQ_SERVER_PUBLIC"])
            server_sec = Protocol::ZMTP::Z85.decode(ENV["OMQ_SERVER_SECRET"])
          else
            key        = crypto::PrivateKey.generate
            server_pub = key.public_key.to_s
            server_sec = key.to_s
          end
          sock.mechanism = Protocol::ZMTP::Mechanism::Curve.server(
            public_key: server_pub,
            secret_key: server_sec,
            crypto: crypto
          )
          $stderr.puts "OMQ_SERVER_KEY='#{Protocol::ZMTP::Z85.encode(server_pub)}'"
        end
      end


      # CLI-level policy: a peer that commits a protocol-level violation
      # (Protocol::ZMTP::Error — oversized frame, decompression bytebomb,
      # bad framing, …) is almost certainly a misconfiguration the user
      # needs to see. Mark +sock+ dead so the next receive raises
      # SocketDeadError. The library itself just drops the connection and
      # keeps serving the others; this stricter policy is CLI-only.
      #
      # @param sock [OMQ::Socket]
      # @param event [OMQ::MonitorEvent]
      #
      def self.kill_on_protocol_error(sock, event)
        return unless event.type == :disconnected
        error = event.detail && event.detail[:error]
        return unless error.is_a?(Protocol::ZMTP::Error)
        sock.engine.signal_fatal_error(error)
      end
    end
  end
end
