# frozen_string_literal: true

require "optparse"
require_relative "cli/version"
require_relative "cli/config"
require_relative "cli/cli_parser"
require_relative "cli/formatter"
require_relative "cli/term"
require_relative "cli/expression_evaluator"
require_relative "cli/socket_setup"
require_relative "cli/routing_helper"
require_relative "cli/transient_monitor"
require_relative "cli/base_runner"
require_relative "cli/push_pull"
require_relative "cli/pub_sub"
require_relative "cli/scatter_gather"
require_relative "cli/radio_dish"
require_relative "cli/req_rep"
require_relative "cli/pair"
require_relative "cli/router_dealer"
require_relative "cli/client_server"
require_relative "cli/ractor_helpers"
require_relative "cli/parallel_worker"
require_relative "cli/pipe_worker"
require_relative "cli/pipe"

module OMQ

  class << self
    # @return [Proc, nil] registered outgoing message transform
    attr_reader :outgoing_proc


    # @return [Proc, nil] registered incoming message transform
    attr_reader :incoming_proc


    # Registers an outgoing message transform (used by -r scripts).
    #
    # @yield [Array<String>] message parts before sending
    # @return [Proc]
    def outgoing(&block)
      @outgoing_proc = block
    end


    # Registers an incoming message transform (used by -r scripts).
    #
    # @yield [Array<String>] message parts after receiving
    # @return [Proc]
    def incoming(&block)
      @incoming_proc = block
    end
  end


  # Command-line interface for OMQ socket operations.
  module CLI
    SOCKET_TYPE_NAMES = %w[
      req rep pub sub push pull pair dealer router
      client server radio dish scatter gather channel peer
      pipe
    ].freeze


    RUNNER_MAP = {
      "push"    => [PushRunner,    :PUSH],
      "pull"    => [PullRunner,    :PULL],
      "pub"     => [PubRunner,     :PUB],
      "sub"     => [SubRunner,     :SUB],
      "req"     => [ReqRunner,     :REQ],
      "rep"     => [RepRunner,     :REP],
      "dealer"  => [PairRunner,    :DEALER],
      "router"  => [RouterRunner,  :ROUTER],
      "pair"    => [PairRunner,    :PAIR],
      "client"  => [ReqRunner,     :CLIENT],
      "server"  => [ServerRunner,  :SERVER],
      "radio"   => [RadioRunner,   :RADIO],
      "dish"    => [DishRunner,    :DISH],
      "scatter" => [ScatterRunner, :SCATTER],
      "gather"  => [GatherRunner,  :GATHER],
      "channel" => [PairRunner,    :CHANNEL],
      "peer"    => [ServerRunner,  :PEER],
      "pipe"    => [PipeRunner,    nil],
    }.freeze


    module_function


    # Displays text through the system pager, or prints directly
    # when stdout is not a terminal.
    #
    def page(text)
      if $stdout.tty?
        if ENV["PAGER"]
          pager = ENV["PAGER"]
        else
          ENV["LESS"] ||= "-FR"
          pager = "less"
        end
        IO.popen(pager, "w") { |io| io.puts text }
      else
        puts text
      end
    rescue Errno::ENOENT
      puts text
    rescue Errno::EPIPE, Interrupt
      # user quit pager early (q) or hit ^C while paging
    end


    # Main entry point: dispatches to keygen or socket runner.
    #
    # @param argv [Array<String>] command-line arguments
    # @return [void]
    def run(argv = ARGV)
      case argv.first
      when "keygen"
        argv.shift
        run_keygen(argv)
      else
        run_socket(argv)
      end
    end


    # Generates a persistent CURVE keypair and prints it as
    # Z85-encoded env vars.
    #
    def run_keygen(argv)
      crypto_name = nil
      verbose     = false

      while (arg = argv.shift)
        case arg
        when "--crypto"
          crypto_name = argv.shift
        when "-v", "--verbose"
          verbose = true
        when "-h", "--help"
          puts "Usage: omq keygen [--crypto rbnacl|nuckle] [-v]\n\n" \
               "Generates a CURVE keypair for persistent server identity.\n" \
               "Output: Z85-encoded env vars for use with --curve-server."
          exit
        else
          abort "omq keygen: unknown option: #{arg}"
        end
      end
      crypto_name ||= ENV["OMQ_CRYPTO"]

      crypto = load_curve_crypto(crypto_name, verbose: verbose)
      require "protocol/zmtp/mechanism/curve"
      require "protocol/zmtp/z85"

      key = crypto::PrivateKey.generate
      puts "OMQ_SERVER_PUBLIC='#{Protocol::ZMTP::Z85.encode(key.public_key.to_s)}'"
      puts "OMQ_SERVER_SECRET='#{Protocol::ZMTP::Z85.encode(key.to_s)}'"
    end


    # Loads the named NaCl-compatible crypto backend.
    #
    # @param name [String, nil] "rbnacl", "nuckle", or nil (auto-detect rbnacl)
    # @param verbose [Boolean] log which backend was loaded to stderr
    # @return [Module] RbNaCl or Nuckle
    #
    def load_curve_crypto(name, verbose: false)
      crypto = case name&.downcase
      when "rbnacl"
        require "rbnacl"
        RbNaCl
      when "nuckle"
        require "nuckle"
        Nuckle
      when nil
        begin
          require "rbnacl"
          RbNaCl
        rescue LoadError
          abort "CURVE requires libsodium. Install it:\n" \
                "  apt install libsodium-dev    # Debian/Ubuntu\n" \
                "  brew install libsodium       # macOS\n" \
                "Or use nuckle (pure Ruby, DANGEROUS -- not audited):\n" \
                "  --crypto nuckle"
        end
      else
        abort "Unknown CURVE crypto backend: #{name}. Use 'rbnacl' or 'nuckle'."
      end
      $stderr.puts "omq: CURVE crypto backend: #{crypto.name}" if verbose
      crypto
    rescue LoadError
      abort "Could not load #{name} gem: gem install #{name}"
    end


    # Parses CLI arguments, validates options, and runs the main
    # event loop inside an Async reactor.
    #
    def run_socket(argv)
      config = build_config(argv)

      require "omq"
      require "omq/client_server"
      require "omq/radio_dish"
      require "omq/scatter_gather"
      require "omq/channel"
      require "omq/peer"
      require "omq/zstd"
      require "omq/lz4"
      require "async"
      require "json"
      require "console"

      CliParser.validate_gems!(config)
      trap("INT")  { Process.exit!(0) }
      trap("TERM") { Process.exit!(0) }

      Console.logger = Console::Logger.new(Console::Output::Null.new) unless config.verbose >= 1

      debug_ep = nil

      if ENV["OMQ_DEBUG_URI"]
        begin
          require "async/debug"
          debug_ep = Async::HTTP::Endpoint.parse(ENV["OMQ_DEBUG_URI"])
          if debug_ep.scheme == "https"
            require "localhost"
            debug_ep = Async::HTTP::Endpoint.parse(ENV["OMQ_DEBUG_URI"],
              ssl_context: Localhost::Authority.fetch.server_context)
          end
        rescue LoadError
          abort "OMQ_DEBUG_URI requires the async-debug gem: gem install async-debug"
        end
      end

      if config.type_name.nil?
        Process.setproctitle("omq script")
        Object.include(OMQ) unless Object.include?(OMQ)
        Async annotation: 'omq' do
          Async::Debug.serve(endpoint: debug_ep) if debug_ep
          config.scripts.each { |s| load_script(s) }
        rescue => e
          $stderr.puts "omq: #{e.message}"
          exit 1
        end
        return
      end

      runner_class, socket_sym = RUNNER_MAP.fetch(config.type_name)

      Async annotation: "omq #{config.type_name}" do |task|
        Async::Debug.serve(endpoint: debug_ep) if debug_ep
        config.scripts.each { |s| load_script(s) }
        runner = if socket_sym
                   runner_class.new(config, OMQ.const_get(socket_sym))
                 else
                   runner_class.new(config)
                 end
        runner.call(task)
      rescue IO::TimeoutError, Async::TimeoutError
        $stderr.puts "omq: timeout" unless config.quiet
        exit 2
      rescue OMQ::SocketDeadError => e
        $stderr.puts "omq: #{e.cause.class}: #{e.cause.message}"
        exit 1
      rescue ::Socket::ResolutionError => e
        $stderr.puts "omq: #{e.message}"
        exit 1
      end
    end


    def load_script(s)
      if s == :stdin
        eval($stdin.read, TOPLEVEL_BINDING, "(stdin)", 1) # rubocop:disable Security/Eval
      else
        require s
      end
    end
    private_class_method :load_script


    # Builds a frozen Config from command-line arguments.
    #
    def build_config(argv)
      opts = CliParser.parse(argv)
      CliParser.validate!(opts)

      opts[:stdin_is_tty] = $stdin.tty?

      Ractor.make_shareable(Config.new(**opts))
    end
  end
end
