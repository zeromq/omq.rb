# frozen_string_literal: true

# OMQ QoS — per-hop delivery guarantees for OMQ sockets.
#
# Usage:
#   require "omq"
#   require "omq/qos"
#
#   push = OMQ::PUSH.new
#   push.qos = OMQ::QoS.exactly_once(dead_letter_timeout: 30)
#   push.connect("tcp://127.0.0.1:5555")
#   promise = push.send("guaranteed delivery")
#   case promise.wait
#   in :delivered                 then :ok
#   in OMQ::QoS::DeadLetter => dl then retry_queue << dl.parts
#   end

require "async/semaphore"

require "omq"

require_relative "qos/version"
require_relative "qos/zmtp/command_ext"
require_relative "qos/hasher"
require_relative "qos/pending_store"
require_relative "qos/peer_registry"
require_relative "qos/dedup_set"
require_relative "qos/dead_letter"
require_relative "qos/error_codes"
require_relative "qos/retry_scheduler"
require_relative "qos/connection_ext"
require_relative "qos/lifecycle_ext"
require_relative "qos/routing_ext"
require_relative "qos/options_ext"
require_relative "qos/socket_ext"

module OMQ
  # Per-hop delivery guarantee handle for a single socket.
  #
  # A {QoS} instance bundles both the user-visible configuration (level,
  # hash-algorithm preference, timeouts, retry caps) and the socket-wide
  # runtime state needed for levels >= 2 ({PeerRegistry}, a bounded-slot
  # {Async::Semaphore} for backpressure, per-connection {DedupSet}s, and
  # the dead-letter sweep task).
  #
  # Construct via the class-method builders — never via `.new(level:)`
  # directly.
  #
  #   socket.qos = nil                                              # QoS 0 (default)
  #   socket.qos = OMQ::QoS.at_least_once                           # QoS 1
  #   socket.qos = OMQ::QoS.exactly_once(dead_letter_timeout: 30)   # QoS 2
  #   socket.qos = OMQ::QoS.exactly_once_and_processed(             # QoS 3
  #     max_retries: 5, processing_timeout: 1.0,
  #   )
  #
  # Each socket gets its own instance. The same {QoS} cannot be shared
  # across sockets — {#attach!} rejects a second engine.
  #
  class QoS
    DEFAULT_DEAD_LETTER_TIMEOUT = 60.0
    DEFAULT_DEDUP_TTL           = 60.0
    DEFAULT_MAX_RETRIES         = 3
    DEFAULT_RETRY_BACKOFF       = (0.1..10.0).freeze


    # Data outcome pushed through `#send`'s Promise when a message
    # cannot be delivered under the requested guarantee.
    #
    # @!attribute [r] parts
    #   @return [Array<String>] the original message frames
    # @!attribute [r] reason
    #   @return [Symbol] +:peer_timeout+, +:terminal_nack+, +:retry_exhausted+, +:socket_closed+
    # @!attribute [r] peer_info
    #   @return [Protocol::ZMTP::PeerInfo, nil] peer it was pinned to, if any
    # @!attribute [r] error
    #   @return [NackInfo, nil] NACK details at QoS 3; nil at QoS 2
    DeadLetter = Data.define(:parts, :reason, :peer_info, :error)


    # Builds a QoS 1 (at-least-once) handle.
    #
    # @param hash_algos [String] preference list, e.g. +"xs"+
    # @return [QoS]
    def self.at_least_once(hash_algos: SUPPORTED_HASH_ALGOS)
      new(level: 1, hash_algos: hash_algos)
    end


    # Builds a QoS 2 (exactly-once with peer pinning) handle.
    #
    # @param hash_algos [String]
    # @param dead_letter_timeout [Numeric] seconds to wait for a disconnected
    #   peer to return before dead-lettering its pending messages
    # @param dedup_ttl [Numeric] seconds a receiver keeps a digest in its
    #   dedup set before it may evict it
    # @return [QoS]
    def self.exactly_once(
      hash_algos:           SUPPORTED_HASH_ALGOS,
      dead_letter_timeout:  DEFAULT_DEAD_LETTER_TIMEOUT,
      dedup_ttl:            DEFAULT_DEDUP_TTL
    )
      new(
        level:               2,
        hash_algos:          hash_algos,
        dead_letter_timeout: dead_letter_timeout,
        dedup_ttl:           dedup_ttl,
      )
    end


    # Builds a QoS 3 (exactly-once + application-level COMP/NACK) handle.
    #
    # @param hash_algos [String]
    # @param dead_letter_timeout [Numeric]
    # @param dedup_ttl [Numeric]
    # @param max_retries [Integer] retry cap on retryable NACKs
    # @param processing_timeout [Numeric, nil] per-message receiver handler deadline
    # @param retry_backoff [Range] exponential backoff bounds (min..max, seconds)
    # @return [QoS]
    def self.exactly_once_and_processed(
      hash_algos:           SUPPORTED_HASH_ALGOS,
      dead_letter_timeout:  DEFAULT_DEAD_LETTER_TIMEOUT,
      dedup_ttl:            DEFAULT_DEDUP_TTL,
      max_retries:          DEFAULT_MAX_RETRIES,
      processing_timeout:   nil,
      retry_backoff:        DEFAULT_RETRY_BACKOFF
    )
      new(
        level:               3,
        hash_algos:          hash_algos,
        dead_letter_timeout: dead_letter_timeout,
        dedup_ttl:           dedup_ttl,
        max_retries:         max_retries,
        processing_timeout:  processing_timeout,
        retry_backoff:       retry_backoff,
      )
    end


    attr_reader :level, :hash_algos,
                :dead_letter_timeout, :dedup_ttl,
                :max_retries, :processing_timeout, :retry_backoff


    # Constructed only via the class-method builders.
    def initialize(level:,
                   hash_algos:          SUPPORTED_HASH_ALGOS,
                   dead_letter_timeout: nil,
                   dedup_ttl:           nil,
                   max_retries:         nil,
                   processing_timeout:  nil,
                   retry_backoff:       nil)
      @level               = level
      @hash_algos          = hash_algos
      @dead_letter_timeout = dead_letter_timeout
      @dedup_ttl           = dedup_ttl
      @max_retries         = max_retries
      @processing_timeout  = processing_timeout
      @retry_backoff       = retry_backoff

      @engine            = nil
      @peer_registry     = nil
      @send_semaphore    = nil
      @dedup_sets        = nil
      @sweep_task        = nil
      @enqueue_promises  = nil
    end


    # Registers the {Async::Promise} associated with an outgoing
    # +parts+ array keyed by +parts.object_id+. The routing-layer
    # {RoundRobinExt#write_batch} looks it up (via
    # {#take_enqueue_promise}) right before handing the entry to the
    # {PeerRegistry}. Using object_id on a freshly-frozen Array that
    # lives in the send queue avoids reshaping the send queue itself
    # (which would otherwise need a Data wrapper every consumer has
    # to know about).
    #
    # @param parts [Array<String>]
    # @param promise [Async::Promise]
    def enqueue_promise(parts, promise)
      (@enqueue_promises ||= {})[parts.object_id] = promise
    end


    # Removes and returns the Promise previously registered for +parts+.
    # nil if none (e.g. message was enqueued before QoS upgrade — should
    # not happen under the rules enforced by {SocketExt#qos=}).
    def take_enqueue_promise(parts)
      @enqueue_promises&.delete(parts.object_id)
    end


    # Binds this QoS handle to a socket's engine. Called by
    # {SocketExt#qos=} once the instance is assigned.
    #
    # @param engine [OMQ::Engine]
    # @return [void]
    # @raise [ArgumentError] if already attached to a (possibly different) engine
    def attach!(engine)
      if @engine && !@engine.equal?(engine)
        raise ArgumentError, "OMQ::QoS instance is already attached to a different socket"
      end
      @engine = engine
    end


    # @return [Boolean]
    def attached?
      !@engine.nil?
    end


    # Lazily built pending-message registry (QoS >= 2). Keyed by
    # {Protocol::ZMTP::PeerInfo} so entries survive reconnects.
    #
    # @return [PeerRegistry]
    def peer_registry
      @peer_registry ||= PeerRegistry.new(capacity: @engine.options.send_hwm)
    end


    # Lazily built bounded-slot semaphore. Bounds the number of
    # in-flight (un-ACK'd / un-COMP'd) messages at +send_hwm+; senders
    # acquire a slot before tracking and release on ACK/COMP/dead-letter.
    #
    # @return [Async::Semaphore]
    def send_semaphore
      @send_semaphore ||= Async::Semaphore.new(@engine.options.send_hwm)
    end


    # Lazily built per-connection dedup set for the receive side (QoS >= 2).
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @return [DedupSet]
    def dedup_set_for(conn)
      (@dedup_sets ||= {})[conn] ||= DedupSet.new(capacity: @engine.options.recv_hwm)
    end


    # Drops the dedup set for a connection (called on disconnect).
    def forget_dedup_set(conn)
      @dedup_sets&.delete(conn)
    end


    # Tears down socket-scoped state. Called from {OMQ::Socket#close}.
    # Resolves any pending Promises with +DeadLetter(reason: :socket_closed)+
    # so no fiber hangs on +#wait+.
    #
    # @return [void]
    def shutdown
      @sweep_task&.stop
      @sweep_task = nil
      @peer_registry&.drain_with_dead_letter(:socket_closed)
    end


    # Spawns a task on the engine's task tree. Used by the QoS 3 retry
    # scheduler and internal sweep fiber.
    #
    # @param annotation [String]
    # @yield the task body
    # @return [Async::Task]
    def spawn_task(annotation:, &block)
      @engine.spawn_pump_task(annotation: annotation, &block)
    end


    # Starts the per-socket dead-letter sweep fiber (idempotent).
    # Called from the routing layer once the first connection reaches
    # the handshake-ready point — that's when +Async::Task.current+ is
    # guaranteed to be inside the engine's task tree.
    #
    # @return [void]
    def ensure_sweep_task_started
      return if @sweep_task
      return unless @level >= 2 && @dead_letter_timeout

      interval = @dead_letter_timeout / 4.0
      @sweep_task = @engine.spawn_pump_task(annotation: "qos dead-letter sweep") do
        loop do
          sleep interval
          next unless @peer_registry

          expired = @peer_registry.sweep_dead_letters(Async::Clock.now, @dead_letter_timeout)
          expired.each do |entry, peer_info|
            QoS.dead_letter(entry, peer_info: peer_info, reason: :peer_timeout, semaphore: @send_semaphore)
          end
        end
      end
    end
  end
end

# Wire up prepends.
Protocol::ZMTP::Codec::Command.singleton_class.prepend(OMQ::QoS::CommandClassExt)
Protocol::ZMTP::Codec::Command.prepend(OMQ::QoS::CommandExt)

OMQ::Routing::RoundRobin.prepend(OMQ::QoS::RoundRobinExt)
OMQ::Routing::Push.prepend(OMQ::QoS::PushExt)
OMQ::Routing::Pull.prepend(OMQ::QoS::PullExt)

# Draft socket types from optional extension gems.
OMQ::Routing::Scatter.prepend(OMQ::QoS::ScatterExt) if defined?(OMQ::Routing::Scatter)
OMQ::Routing::Gather.prepend(OMQ::QoS::GatherExt)   if defined?(OMQ::Routing::Gather)
