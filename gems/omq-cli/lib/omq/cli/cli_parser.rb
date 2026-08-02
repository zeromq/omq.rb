# frozen_string_literal: true

require "socket"
require "etc"

module OMQ
  module CLI
    # Parses and validates command-line arguments for the omq CLI.
    #
    class CliParser
      EXAMPLES = <<~'TEXT'
        -- Request / Reply ------------------------------------------

          +-----+   "hello"    +-----+
          | REQ |------------->| REP |
          |     |<-------------|     |
          +-----+   "HELLO"    +-----+

          # terminal 1: echo server
          omq rep --bind tcp://:5555 --recv-eval 'it.map(&:upcase)'

          # terminal 2: send a request
          echo "hello" | omq req --connect tcp://localhost:5555

          # or over IPC (unix socket, single machine)
          omq rep --bind ipc:///tmp/echo.sock --echo &
          echo "hello" | omq req --connect ipc:///tmp/echo.sock

        -- Publish / Subscribe --------------------------------------

          +-----+  "weather.nyc 72F"  +-----+
          | PUB |-------------------->| SUB | --subscribe "weather."
          +-----+                     +-----+

          # terminal 1: subscriber (all topics by default)
          omq sub --bind tcp://:5556

          # terminal 2: publisher (needs --delay for subscription to propagate)
          echo "weather.nyc 72F" | omq pub --connect tcp://localhost:5556 --delay 1

        -- Periodic Publish -------------------------------------------

          +-----+   "tick 1"    +-----+
          | PUB |--(every 1s)-->| SUB |
          +-----+               +-----+

          # terminal 1: subscriber
          omq sub --bind tcp://:5556

          # terminal 2: publish a tick every second (wall-clock aligned)
          omq pub --connect tcp://localhost:5556 --delay 1 --data "tick" --interval 1

          # 5 ticks, then exit
          omq pub --connect tcp://localhost:5556 -d1 -D "tick" -i0.5 --count 5

        -- Pipeline -------------------------------------------------

          +------+           +------+
          | PUSH |---------->| PULL |
          +------+           +------+

          # terminal 1: worker
          omq pull --bind tcp://:5557

          # terminal 2: send tasks
          echo "task 1" | omq push --connect tcp://localhost:5557

          # or over IPC (unix socket)
          omq pull --bind ipc:///tmp/pipeline.sock &
          echo "task 1" | omq push --connect ipc:///tmp/pipeline.sock

        -- Pipe (PULL -> eval -> PUSH) --------------------------------

          +------+         +------+         +------+
          | PUSH |-------->| pipe |-------->| PULL |
          +------+         +------+         +------+

          # @work / @sink below are Linux abstract-namespace unix
          # sockets (ipc://@name) -- no path on disk, cleaned up
          # automatically. Linux only. Use ipc:///tmp/work etc.
          # on macOS/BSD.

          # terminal 1: producer
          echo -e "hello\nworld" | omq push -b@work

          # terminal 2: worker (uppercase each message)
          omq pipe -c@work -c@sink -e 'it.map(&:upcase)'
          # terminal 3: collector
          omq pull -b@sink

          # 4 Ractor workers in a single process (-P)
          omq pipe -c@work -c@sink -P4 -r./fib -e 'fib(it.first.to_i).to_s'

          # exit when producer disconnects (--transient)
          omq pipe -c@work -c@sink --transient -e 'it.map(&:upcase)'

          # fan-in (multiple sources -> one sink)
          omq pipe --in -c@work1 -c@work2 --out -c@sink -e 'it.map(&:upcase)'

          # fan-out (one source -> multiple sinks, round-robin)
          omq pipe --in -b tcp://:5555 --out -c@sink1 -c@sink2 -e 'it'

        -- CLIENT / SERVER (draft) ----------------------------------

          +--------+   "hello"   +--------+
          | CLIENT |------------>| SERVER | --recv-eval 'it.map(&:upcase)'
          |        |<------------|        |
          +--------+   "HELLO"   +--------+

          # terminal 1: upcasing server
          omq server --bind tcp://:5555 --recv-eval 'it.map(&:upcase)'

          # terminal 2: client
          echo "hello" | omq client --connect tcp://localhost:5555

        -- Formats --------------------------------------------------

          # ascii (default) -- non-printable replaced with dots
          omq pull --bind tcp://:5557 --ascii

          # quoted -- lossless, round-trippable (uses String#dump escaping)
          omq pull --bind tcp://:5557 --quoted

          # JSON Lines -- structured, multipart as arrays
          echo '["key","value"]' | omq push --connect tcp://localhost:5557 --jsonl
          omq pull --bind tcp://:5557 --jsonl

          # multipart via tabs
          printf "routing-key\tpayload" | omq push --connect tcp://localhost:5557

        -- Marshal (arbitrary Ruby objects) -------------------------

          # -M: each message is one Marshal-dumped Ruby object.
          # Inside -e/-E, `it` is the raw object (not an Array).

          # send a bare string; receiver transforms it to { string => encoding } hash
          omq push -b tcp://:5557 -ME '"foo" * 3'
          omq pull -c tcp://:5557 -Me '{it => it.encoding}'
          # output: {"foofoofoo" => #<Encoding:UTF-8>}

          # -vvv traces render the app object, not wire bytes
          omq push -b tcp://:5557 -ME '{now: Time.now, pid: Process.pid}' -vvv
          # >> (marshal) {now: 2026-04-13 ..., pid: 12345}

        -- Compression ----------------------------------------------

          # -z / -Z / --lz4 rewrite tcp:// endpoints to zstd+tcp:// or
          # lz4+tcp:// (dedicated compressed transports from omq-zstd
          # and omq-lz4). Both peers must agree on the codec; non-TCP
          # endpoints are unaffected.
          omq pull --bind tcp://:5557 -z &
          echo "compressible data" | omq push --connect tcp://localhost:5557 -z

          # LZ4: no entropy stage, ~4–8× faster encode than zstd,
          # ~3× less memory. Pick it when CPU or RAM is tighter than
          # bandwidth.
          omq pull --bind tcp://:5557 --lz4 &
          echo "hello" | omq push --connect tcp://localhost:5557 --lz4

          # Explicit spec form (same for -e, -f, or --compress):
          omq pull -b tcp://:5557 --compress=zstd:3
          omq pull -b tcp://:5557 --compress=lz4

          # Pipe with asymmetric codecs (decompresses lz4 in, re-emits zstd out):
          omq pipe --in --lz4 -c tcp://src:5555 --out -z -c tcp://dst:6666

        -- CURVE Encryption -----------------------------------------

          # server (prints OMQ_SERVER_KEY=...)
          omq rep --bind tcp://:5555 --echo --curve-server

          # client (paste the server's key)
          echo "secret" | omq req --connect tcp://localhost:5555 \
            --curve-server-key '<key from server>'

        -- ROUTER / DEALER ------------------------------------------

          +--------+          +--------+
          | DEALER |--------->| ROUTER |
          | id=w1  |          |        |
          +--------+          +--------+

          # terminal 1: router shows identity + message
          omq router --bind tcp://:5555

          # terminal 2: dealer with identity
          echo "hello" | omq dealer --connect tcp://localhost:5555 --identity worker-1

        -- Ruby Eval ------------------------------------------------

          # filter incoming: only pass messages containing "error"
          omq pull -b tcp://:5557 --recv-eval 'it.first.include?("error") ? it : nil'

          # transform incoming with gems
          omq sub -c tcp://localhost:5556 -rjson -e 'JSON.parse(it.first)["temperature"]'

          # require a local file, use its methods
          omq rep --bind tcp://:5555 --require ./transform.rb -e 'upcase_all(it)'

          # next skips, break stops
          omq pull -b tcp://:5557 -e 'next if it.first =~ /^#/; break if it.first =~ /quit/; it'

          # BEGIN/END blocks (like awk) -- accumulate and summarize
          omq pull -b tcp://:5557 -e 'BEGIN{@sum = 0} @sum += it.first.to_i; nil END{puts @sum}'

          # transform outgoing messages
          echo hello | omq push -c tcp://localhost:5557 --send-eval 'it.map(&:upcase)'

          # REQ: transform request and reply independently
          echo hello | omq req -c tcp://localhost:5555 -E 'it.map(&:upcase)' -e 'it.first'

          # block parameter: single param receives parts array
          omq pull -b tcp://:5557 -e '|msg| msg.map(&:upcase)'

          # destructure multipart messages with parens
          omq pull -b tcp://:5557 -e '|(key, value)| "#{key}=#{value}"'

        -- Script Handlers (-r) ------------------------------------

          # handler.rb -- register transforms from a file
          #   db = PG.connect("dbname=app")
          #   OMQ.incoming { |first_part, _| db.exec(first_part).values.flatten }
          #   at_exit { db.close }
          omq pull --bind tcp://:5557 -r./handler.rb

          # combine script handlers with inline eval
          omq req -c tcp://localhost:5555 -r./handler.rb -E 'it.map(&:upcase)'

          # OMQ.outgoing { |msg| ... }   -- registered outgoing transform
          # OMQ.incoming { |msg| ... }   -- registered incoming transform
          # CLI flags (-e/-E) override registered handlers
      TEXT


      DEFAULT_OPTS = {
        type_name:        nil,
        endpoints:        [],
        connects:         [],
        binds:            [],
        in_endpoints:     [],
        out_endpoints:    [],
        data:             nil,
        file:             nil,
        format:           :ascii,
        subscribes:       [],
        joins:            [],
        group:            nil,
        identity:         nil,
        target:           nil,
        interval:         nil,
        count:            nil,
        delay:            nil,
        timeout:          nil,
        linger:           5,
        reconnect_ivl:    nil,
        heartbeat_ivl:    nil,
        send_hwm:         nil,
        recv_hwm:         nil,
        sndbuf:           nil,
        rcvbuf:           nil,
        conflate:         false,
        compress:         nil,   # nil | :zstd | :lz4
        compress_level:   nil,   # Integer (zstd only)
        in_compress:        nil,
        in_compress_level:  nil,
        out_compress:       nil,
        out_compress_level: nil,
        send_expr:        nil,
        recv_expr:        nil,
        parallel:         nil,
        transient:        false,
        verbose:          0,
        timestamps:       nil,
        quiet:            false,
        echo:             false,
        scripts:          [],
        recv_maxsz:       nil,
        curve_server:     false,
        curve_server_key: nil,
        crypto:           nil,
        backend:          :ruby,
        ffi:              false,
      }.freeze


      # Parses +argv+ and returns a mutable options hash.
      #
      def self.parse(argv)
        new.parse(argv)
      end


      # Splits short-option clusters of the form `-P[digits][letters]`
      # so OptionParser sees `-P[digits]` followed by `-[letters]`.
      # Lets `-P0zvv` mean `-P0 -z -v -v` (portable & combinable).
      # Also rewrites bare `--timestamps` to `--timestamps=ms` so
      # OptionParser doesn't consume the next positional token as its
      # argument.
      #
      def split_parallel_cluster(argv)
        argv.flat_map { |a|
          if a =~ /\A-P(\d*)([a-zA-Z].*)\z/
            n, rest = $1, $2
            n.empty? ? ["-P", "-#{rest}"] : ["-P#{n}", "-#{rest}"]
          else
            a
          end
        }.map { |a| a == "--timestamps" ? "--timestamps=ms" : a }
      end


      # Validates option combinations, aborting on bad combos.
      #
      def self.validate!(opts)
        new.validate!(opts)
      end


      # Validates option combinations that depend on socket type.
      #
      def self.validate_gems!(config)
        if config.recv_only? && (config.data || config.file)
          abort "--data/--file not valid for #{config.type_name} (receive-only)"
        end
      end


      # Parses +argv+ and returns a mutable options hash.
      #
      # @param argv [Array<String>] command-line arguments (mutated in place)
      # @return [Hash] parsed options
      def parse(argv)
        opts      = DEFAULT_OPTS.transform_values { |v| v.is_a?(Array) ? v.dup : v }
        pipe_side = nil  # nil = legacy positional mode; :in/:out = modal
        explicit_backend = false

        parser = OptionParser.new do |o|
          o.banner = "Usage: omq TYPE [options]\n\n" \
                     "Types:    req, rep, pub, sub, push, pull, pair, dealer, router\n" \
                     "Draft:    client, server, radio, dish, scatter, gather, channel, peer\n" \
                     "Virtual:  pipe (PULL -> eval -> PUSH)\n\n"

          o.separator "Connection:"
          o.on("-c", "--connect URL", "Connect to endpoint (repeatable)") { |v|
            v = expand_endpoint(v)
            ep = Endpoint.new(v, false)
            case pipe_side
            when :in
              opts[:in_endpoints] << ep
            when :out
              opts[:out_endpoints] << ep
            else
              opts[:endpoints] << ep
              opts[:connects]  << v
            end
          }
          o.on("-b", "--bind URL", "Bind to endpoint (repeatable)") { |v|
            v = expand_endpoint(v)
            ep = Endpoint.new(v, true)
            case pipe_side
            when :in
              opts[:in_endpoints] << ep
            when :out
              opts[:out_endpoints] << ep
            else
              opts[:endpoints] << ep
              opts[:binds]     << v
            end
          }
          o.on("--in",  "Pipe: subsequent -b/-c attach to input (PULL) side")  { pipe_side = :in }
          o.on("--out", "Pipe: subsequent -b/-c attach to output (PUSH) side") { pipe_side = :out }

          o.separator "\nData source (REP: reply source):"
          o.on(      "--echo",        "Echo received messages back (REP)")   { opts[:echo] = true }
          o.on("-D", "--data DATA",   "Message data (literal string)")      { |v| opts[:data] = v }
          o.on("-F", "--file FILE",   "Read message from file (- = stdin)") { |v| opts[:file] = v }

          o.separator "\nFormat (input + output):"
          o.on("-A", "--ascii",   "Tab-separated frames, safe ASCII (default)") { opts[:format] = :ascii }
          o.on("-Q", "--quoted",  "C-style quoted with escapes")                { opts[:format] = :quoted }
          o.on(      "--raw",     "Raw binary, no framing")                     { opts[:format] = :raw }
          o.on("-J", "--jsonl",   "JSON Lines (array of strings per line)")     { opts[:format] = :jsonl }
          o.on(      "--msgpack",  "MessagePack arrays (binary stream)")         { require "msgpack"; opts[:format] = :msgpack }
          o.on("-M", "--marshal", "Ruby Marshal stream (one arbitrary object per message)") { opts[:format] = :marshal }

          o.separator "\nSubscription/groups:"
          o.on("-s", "--subscribe PREFIX", "Subscribe prefix (SUB, default all)")     { |v| opts[:subscribes] << v }
          o.on("-j", "--join GROUP",       "Join group (repeatable, DISH only)")      { |v| opts[:joins] << v }
          o.on("-g", "--group GROUP",      "Publish group (RADIO only)")              { |v| opts[:group] = v }

          o.separator "\nIdentity/routing:"
          o.on("--identity ID", "Set socket identity (DEALER/ROUTER)")                     { |v| opts[:identity] = v }
          o.on("--target ID",   "Target peer (ROUTER/SERVER/PEER, 0x prefix for binary)")  { |v| opts[:target] = v }

          o.separator "\nTiming:"
          o.on("-i", "--interval SECS", Float,   "Repeat interval")                   { |v| opts[:interval] = v }
          o.on("-n", "--count COUNT",   Integer,  "Max iterations (0=inf)")            { |v| opts[:count] = v }
          o.on("-d", "--delay SECS",    Float,   "Delay before first send")            { |v| opts[:delay] = v }
          o.on("-t", "--timeout SECS",  Float,   "Send/receive timeout")               { |v| opts[:timeout] = v }
          o.on("-l", "--linger SECS",   Float,   "Drain time on close (default 5)")   { |v| opts[:linger] = v }
          o.on("--reconnect-ivl IVL", "Reconnect interval: SECS or MIN..MAX (default 0.1)") { |v|
            opts[:reconnect_ivl] = if v.include?("..")
                                     lo, hi = v.split("..", 2)
                                     Float(lo)..Float(hi)
                                   else
                                     Float(v)
                                   end
          }
          o.on("--heartbeat-ivl SECS", Float, "ZMTP heartbeat interval (detects dead peers)") { |v| opts[:heartbeat_ivl] = v }
          o.on("--recv-maxsz SIZE", "Max inbound message size, e.g. 4096, 64K, 1M, 2G (default 1M, 0=unlimited; larger messages drop the connection)") { |v| opts[:recv_maxsz] = parse_byte_size(v) }
          o.on("--hwm N", Integer, "High water mark (default 64, 0=unbounded; modal with --in/--out)") do |v|
            case pipe_side
            when :in
              opts[:recv_hwm] = v
            when :out
              opts[:send_hwm] = v
            else
              opts[:send_hwm] = v
              opts[:recv_hwm] = v
            end
          end
          o.on("--sndbuf N", "SO_SNDBUF kernel buffer size (e.g. 4K, 1M)") { |v| opts[:sndbuf] = parse_byte_size(v) }
          o.on("--rcvbuf N", "SO_RCVBUF kernel buffer size (e.g. 4K, 1M)") { |v| opts[:rcvbuf] = parse_byte_size(v) }

          o.separator "\nDelivery:"
          o.on("--conflate", "Keep only last message per subscriber (PUB/RADIO)") { opts[:conflate] = true }

          o.separator "\nCompression (modal with --in/--out for pipe):"
          set_compress = lambda do |codec, level|
            case pipe_side
            when :in
              opts[:in_compress] = codec
              opts[:in_compress_level] = level
            when :out
              opts[:out_compress] = codec
              opts[:out_compress_level] = level
            else
              opts[:compress] = codec
              opts[:compress_level] = level
            end
          end
          o.on("-z",    "Zstd compression (level -3, fast)")        { set_compress.call(:zstd, -3) }
          o.on("-Z",    "Zstd compression (level 3, better ratio)") { set_compress.call(:zstd, 3) }
          o.on("--lz4", "LZ4 compression (FAST)")                   { set_compress.call(:lz4, nil) }
          o.on("--compress=SPEC", "Explicit codec[:level], e.g. zstd, zstd:3, zstd:-1, lz4") do |v|
            codec, level = parse_compress_spec(v)
            set_compress.call(codec, level)
          end

          o.separator "\nProcessing (-e = incoming, -E = outgoing):"
          o.on("-e", "--recv-eval EXPR", "Eval Ruby for each incoming message (it = parts, or |a, b|)") { |v| opts[:recv_expr] = v }
          o.on("-E", "--send-eval EXPR", "Eval Ruby for each outgoing message (it = parts, or |a, b|)") { |v| opts[:send_expr] = v }
          o.on("-r", "--require LIB",  "Require lib/file in Async context; use '-' for stdin. Scripts can register OMQ.outgoing/incoming") { |v|
            require "omq" unless defined?(OMQ::VERSION)
            opts[:scripts] << (v == "-" ? :stdin : (v.start_with?("./", "../") ? File.expand_path(v) : v))
          }
          o.on("-P", "--parallel [N]", Integer, "Parallel Ractor workers (0 = nproc, max 16)") { |v|
            n = v.nil? || v.zero? ? Etc.nprocessors : v
            opts[:parallel] = [n, 16].min
          }

          o.separator "\nCURVE encryption (requires system libsodium):"
          o.on("--curve-server",         "Enable CURVE as server (generates keypair)") { opts[:curve_server] = true }
          o.on("--curve-server-key KEY", "Enable CURVE as client (server's Z85 public key)") { |v| opts[:curve_server_key] = v }
          o.on("--crypto BACKEND", "Crypto backend: rbnacl (default) or nuckle (pure Ruby, DANGEROUS)") { |v| opts[:crypto] = v }
          o.separator "  Install libsodium: apt install libsodium-dev / brew install libsodium"

          o.separator "\nOther:"
          o.on("-v", "--verbose",   "Verbosity: -v endpoints, -vv events, -vvv messages") { opts[:verbose] += 1 }
          o.on(      "--timestamps PRECISION", %w[s ms us], "Prefix log lines with UTC timestamp (s/ms/us, default ms)") { |v|
            opts[:timestamps] = v.to_sym
          }
          o.on("-q", "--quiet",     "Suppress message output")           { opts[:quiet] = true }
          o.on(      "--transient", "Exit when all peers disconnect")    { opts[:transient] = true }
          o.on(      "--backend BACKEND", %w[ruby rust libzmq ffi],
                     "Socket backend: ruby, rust, or libzmq") do |v|
            backend = normalize_backend(v.to_sym)
            load_socket_backend!(backend)
            opts[:backend] = backend
            opts[:ffi] = false
            explicit_backend = true
          end
          o.on(      "--ffi",       "Alias for --backend libzmq") do
            load_socket_backend!(:libzmq)
            opts[:backend] = :libzmq
            opts[:ffi] = true
            explicit_backend = true
          end
          o.on("-V", "--version") {
            require "omq/version"
            puts "omq-cli #{OMQ::CLI::VERSION} (omq #{OMQ::VERSION})"
            exit
          }
          o.on("-h")             { puts o
                                   exit }
          o.on("--help")        { CLI.page "#{o}\n#{EXAMPLES}"
                                   exit }
          o.on("--examples")    { CLI.page EXAMPLES
                                   exit }

          o.separator "\nEnv vars:"
          o.separator "  OMQ_BACKEND (socket backend: ruby, rust, or libzmq)"
          o.separator "  OMQ_SERVER_KEY (client CURVE server key)"
          o.separator "  OMQ_SERVER_PUBLIC + OMQ_SERVER_SECRET (server CURVE keypair)"
          o.separator "  OMQ_CRYPTO (CURVE crypto backend: rbnacl or nuckle)"

          o.separator "\nExit codes: 0 = success, 1 = error, 2 = timeout"
        end

        argv = split_parallel_cluster(argv)

        begin
          parser.parse!(argv)
        rescue OptionParser::ParseError => e
          abort e.message
        end

        type_name = argv.shift
        if type_name.nil?
          abort parser.to_s if opts[:scripts].empty?
          # bare script mode -- type_name stays nil
        elsif !SOCKET_TYPE_NAMES.include?(type_name.downcase)
          abort "Unknown socket type: #{type_name}. Known: #{SOCKET_TYPE_NAMES.join(', ')}"
        else
          opts[:type_name] = type_name.downcase
        end

        apply_env_backend!(opts) if opts[:type_name] && !explicit_backend

        # Host shorthand (tcp://*:PORT, tcp://:PORT, tcp://localhost:PORT)
        # is normalized inside OMQ::Transport::TCP — see its
        # #normalize_bind_host / #normalize_connect_host / #loopback_host.

        opts
      end


      # Parses a --compress SPEC string.
      #
      # Accepted forms:
      #   "zstd"         → [:zstd, nil]  (SocketSetup picks the default)
      #   "zstd:3"       → [:zstd, 3]
      #   "zstd:-1"      → [:zstd, -1]
      #   "lz4"          → [:lz4, nil]
      #   "-1" | "19"    → [:zstd, N]    (back-compat with 0.16.x --compress=LEVEL)
      #
      # Aborts on anything else (including "lz4:N", since the lz4+tcp
      # transport has no compression-level knob).
      #
      def parse_compress_spec(str)
        case str
        when /\Azstd\z/            then [:zstd, nil]
        when /\Azstd:(-?\d+)\z/    then [:zstd, $1.to_i]
        when /\Alz4\z/             then [:lz4, nil]
        when /\A-?\d+\z/           then [:zstd, str.to_i]
        else
          abort "invalid --compress spec: #{str} (use zstd, zstd:LEVEL, or lz4)"
        end
      end


      # Parses a byte size string with an optional K/M/G suffix (binary,
      # i.e. 1K = 1024 bytes).
      #
      # @param str [String] e.g. "4096", "4K", "1M", "2G"
      # @return [Integer] size in bytes
      #
      def parse_byte_size(str)
        case str
        when /\A(\d+)[kK]\z/ then $1.to_i * 1024
        when /\A(\d+)[mM]\z/ then $1.to_i * 1024 * 1024
        when /\A(\d+)[gG]\z/ then $1.to_i * 1024 * 1024 * 1024
        when /\A\d+\z/       then str.to_i
        else
          abort "invalid byte size: #{str} (use e.g. 4096, 4K, 1M, 2G)"
        end
      end


      # Loads an optional OMQ socket backend while parsing backend flags.
      #
      def load_socket_backend!(backend)
        case backend
        when :ruby
          nil
        when :rust
          require "omq/backend/rust"
        when :libzmq
          require "omq/backend/libzmq"
        end
      rescue LoadError => e
        case backend
        when :rust
          abort "omq: --backend rust requires the omq-backend-rust gem (#{e.message})"
        when :libzmq
          abort "omq: --backend #{backend} requires the omq-backend-libzmq gem and system libzmq 4.x (#{e.message})"
        else
          abort "omq: could not load backend #{backend} (#{e.message})"
        end
      end


      # Applies OMQ_BACKEND when no explicit backend flag was given.
      #
      def apply_env_backend!(opts)
        env_backend = ENV["OMQ_BACKEND"]
        return if env_backend.nil? || env_backend.empty?

        unless %w[ruby rust libzmq].include?(env_backend)
          abort "omq: invalid OMQ_BACKEND=#{env_backend.inspect} (use ruby, rust, or libzmq)"
        end

        backend = env_backend.to_sym
        load_socket_backend!(backend)
        opts[:backend] = backend
        opts[:ffi] = false
      end


      # Maps legacy CLI spelling to the backend name supported by omq.
      #
      def normalize_backend(backend)
        backend == :ffi ? :libzmq : backend
      end


      # Validates option combinations, aborting on invalid combos.
      #
      # @param opts [Hash] parsed options from {#parse}
      # @return [void]
      def validate!(opts)
        return if opts[:type_name].nil?  # bare script mode

        abort "-r- (stdin script) and -F- (stdin data) cannot both be used" if opts[:scripts]&.include?(:stdin) && opts[:file] == "-"

        type_name = opts[:type_name]

        if type_name == "pipe"
          has_in_out = opts[:in_endpoints].any? || opts[:out_endpoints].any?
          if has_in_out
            # Promote bare endpoints into the missing side:
            # `pipe -c SRC --out -c DST` → bare SRC becomes --in
            if opts[:in_endpoints].empty? && opts[:endpoints].any?
              opts[:in_endpoints] = opts[:endpoints]
              opts[:endpoints]    = []
            elsif opts[:out_endpoints].empty? && opts[:endpoints].any?
              opts[:out_endpoints] = opts[:endpoints]
              opts[:endpoints]     = []
            end
            abort "pipe --in requires at least one endpoint"             if opts[:in_endpoints].empty?
            abort "pipe --out requires at least one endpoint"            if opts[:out_endpoints].empty?
            abort "pipe: don't mix --in/--out with bare -b/-c endpoints" unless opts[:endpoints].empty?
          else
            abort "pipe requires exactly 2 endpoints (pull-side and push-side), or use --in/--out" if opts[:endpoints].size != 2
          end
        else
          abort "--in/--out are only valid for pipe" if opts[:in_endpoints].any? || opts[:out_endpoints].any? || opts[:in_compress] || opts[:out_compress]
          abort "At least one --connect or --bind is required" if opts[:connects].empty? && opts[:binds].empty?
        end
        abort "--data and --file are mutually exclusive"        if opts[:data] && opts[:file]
        abort "--subscribe is only valid for SUB"               if !opts[:subscribes].empty? && type_name != "sub"
        abort "--join is only valid for DISH"                   if !opts[:joins].empty? && type_name != "dish"
        abort "--group is only valid for RADIO"                 if opts[:group] && type_name != "radio"
        abort "--identity is only valid for DEALER/ROUTER"      if opts[:identity] && !%w[dealer router].include?(type_name)
        abort "--target is only valid for ROUTER/SERVER/PEER"   if opts[:target] && !%w[router server peer].include?(type_name)
        abort "--conflate is only valid for PUB/RADIO"          if opts[:conflate] && !%w[pub radio].include?(type_name)
        abort "--recv-eval is not valid for send-only sockets (use --send-eval / -E)" if opts[:recv_expr] && SEND_ONLY.include?(type_name)
        abort "--send-eval is not valid for recv-only sockets (use --recv-eval / -e)" if opts[:send_expr] && RECV_ONLY.include?(type_name)
        abort "--send-eval is not valid for REP (the reply is the result of --recv-eval / -e)" if opts[:send_expr] && type_name == "rep"
        abort "--send-eval and --target are mutually exclusive"  if opts[:send_expr] && opts[:target]

        if opts[:parallel]
          parallel_types = %w[pipe pull gather rep]
          abort "-P/--parallel is only valid for #{parallel_types.join(", ")}" unless parallel_types.include?(type_name)
          abort "-P/--parallel must be 1..16" unless (1..16).include?(opts[:parallel])
          if type_name == "pipe"
            all_eps = opts[:in_endpoints] + opts[:out_endpoints] + opts[:endpoints]
          else
            all_eps = opts[:endpoints]
          end
          abort "-P/--parallel requires all endpoints to use --connect (not --bind)" if all_eps.any?(&:bind?)
        end

        (opts[:connects] + opts[:binds]).each do |url|
          abort "ruby:// not supported in CLI, use tcp:// or ipc://" if url.include?("ruby://")
        end

        all_urls = if type_name == "pipe"
                     (opts[:in_endpoints] + opts[:out_endpoints] + opts[:endpoints]).map(&:url)
                   else
                     opts[:connects] + opts[:binds]
                   end
        dups = all_urls.tally.select { |_, n| n > 1 }.keys
        abort "duplicate endpoint: #{dups.first}" if dups.any?
      end


      # Expands shorthand `@name` to `ipc://@name` (Linux abstract namespace).
      # Only triggers when the value starts with `@` and has no `://` scheme.
      #
      def expand_endpoint(url)
        url.start_with?("@") && !url.include?("://") ? "ipc://#{url}" : url
      end


    end
  end
end
