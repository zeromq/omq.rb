# frozen_string_literal: true

module OMQ
  # Pure Ruby socket options.
  #
  # All timeouts are in seconds (Numeric) or nil (no timeout).
  # HWM values are integers.
  #
  class Options
    DEFAULT_HWM = 1000


    # @param linger [Numeric] linger period in seconds on close
    #   (default Float::INFINITY = wait forever, matching libzmq).
    #   Pass 0 for immediate drop-on-close.
    #
    def initialize(linger: Float::INFINITY)
      @send_hwm              = DEFAULT_HWM
      @recv_hwm              = DEFAULT_HWM
      @linger                = linger
      @identity              = "".b
      @router_mandatory      = false
      @read_timeout          = nil   # seconds, nil = no timeout
      @write_timeout         = nil
      @reconnect_interval    = 0.1   # seconds; Range sets backoff min..max
      @heartbeat_interval    = nil   # seconds, nil = disabled
      @heartbeat_ttl         = nil   # seconds, nil = use heartbeat_interval
      @heartbeat_timeout     = nil   # seconds, nil = use heartbeat_interval
      @max_message_size      = nil      # bytes, nil = unlimited
      @max_message_parts     = 1024     # frames per received message, nil = unlimited
      @conflate              = false
      @sndbuf                = nil   # bytes, nil = OS default
      @rcvbuf                = nil   # bytes, nil = OS default
      @on_mute               = :block   # :block, :drop_newest, :drop_oldest
      @mechanism             = Protocol::ZMTP::Mechanism::Null.new
    end


    # @!attribute send_hwm
    #   @return [Integer] send high water mark (default 1000, 0 = unbounded)
    # @!attribute recv_hwm
    #   @return [Integer] receive high water mark (default 1000, 0 = unbounded)
    # @!attribute linger
    #   @return [Numeric] linger period in seconds on close
    #     (Float::INFINITY = wait forever, 0 = immediate drop)
    # @!attribute identity
    #   @return [String] socket identity for ROUTER addressing (default "")
    # @!attribute router_mandatory
    #   @return [Boolean] raise on unroutable messages (default false)
    # @!attribute conflate
    #   @return [Boolean] keep only the latest message per topic (default false)
    # @!attribute read_timeout
    #   @return [Numeric, nil] read timeout in seconds (nil = no timeout)
    # @!attribute write_timeout
    #   @return [Numeric, nil] write timeout in seconds (nil = no timeout)
    # @!attribute heartbeat_interval
    #   @return [Numeric, nil] PING interval in seconds (nil = disabled)
    # @!attribute heartbeat_ttl
    #   @return [Numeric, nil] TTL advertised in PING (nil = use heartbeat_interval)
    # @!attribute heartbeat_timeout
    #   @return [Numeric, nil] time without traffic before closing (nil = use heartbeat_interval)
    # @!attribute max_message_size
    #   @return [Integer, nil] maximum message size in bytes
    # @!attribute max_message_parts
    #   @return [Integer, nil] most frames a received message may have
    #     (default 1024, like omq.rs; nil = unlimited). A peer that sends
    #     more is disconnected.
    # @!attribute sndbuf
    #   @return [Integer, nil] SO_SNDBUF size in bytes (nil = OS default)
    # @!attribute rcvbuf
    #   @return [Integer, nil] SO_RCVBUF size in bytes (nil = OS default)
    # @!attribute on_mute
    #   @return [Symbol] mute strategy (:block, :drop_newest, :drop_oldest)
    # @!attribute mechanism
    #   @return [Protocol::ZMTP::Mechanism::Null, Protocol::ZMTP::Mechanism::Curve] security mechanism
    #
    attr_accessor :send_hwm,  :recv_hwm,
                  :linger,    :identity,
                  :router_mandatory,  :conflate,
                  :read_timeout,          :write_timeout,
                  :heartbeat_interval,    :heartbeat_ttl,    :heartbeat_timeout,
                  :max_message_size,      :max_message_parts,
                  :sndbuf,    :rcvbuf,
                  :on_mute,
                  :mechanism


    # @return [Numeric, Range] reconnect interval in seconds, or Range
    #   for exponential backoff (begin..max)
    #
    attr_reader :reconnect_interval


    # Sets the reconnect interval. An endless Range backs off without a
    # cap and is stored with a +Float::INFINITY+ end.
    #
    # @param value [Numeric, Range] interval in seconds, or begin..max
    # @raise [ArgumentError] unless the interval (or Range begin) is a
    #   positive finite number and the Range end is not below its begin
    #
    def reconnect_interval=(value)
      @reconnect_interval = validate_reconnect_interval(value)
    end


    # @return [Boolean] true if router_mandatory is set
    alias_method :router_mandatory?, :router_mandatory

    # @return [Numeric, nil] alias for #read_timeout
    alias_method :recv_timeout,      :read_timeout

    # @param val [Numeric, nil] alias for #read_timeout=
    alias_method :recv_timeout=,     :read_timeout=

    # @return [Numeric, nil] alias for #write_timeout
    alias_method :send_timeout,      :write_timeout

    # @param val [Numeric, nil] alias for #write_timeout=
    alias_method :send_timeout=,     :write_timeout=


    private


    def validate_reconnect_interval(value)
      case value
      when Range
        min = value.begin
        max = value.end || Float::INFINITY

        unless positive_finite?(min) && max.is_a?(Numeric) && max >= min
          raise ArgumentError, "reconnect_interval Range needs a positive finite begin and an end >= begin, got #{value.inspect}"
        end

        min..max
      when Numeric
        unless positive_finite?(value)
          raise ArgumentError, "reconnect_interval must be a positive finite number, got #{value.inspect}"
        end

        value
      else
        raise ArgumentError, "reconnect_interval must be a Numeric or a Range, got #{value.inspect}"
      end
    end


    def positive_finite?(value)
      value.is_a?(Numeric) && value.positive? && value.finite?
    end

  end
end
