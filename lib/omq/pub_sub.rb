# frozen_string_literal: true

module OMQ
  # PUB socket — publish messages to all matching subscribers.
  #
  class PUB < Socket
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param on_mute [Symbol] mute strategy for slow subscribers
    # @param conflate [Boolean] keep only latest message per topic
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY,
                   send_hwm: nil, send_timeout: nil,
                   on_mute: :drop_newest, conflate: false, backend: nil, &block)
      init_engine(:PUB, send_hwm: send_hwm, send_timeout: send_timeout,
                  on_mute: on_mute, conflate: conflate, backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :bind)
      finalize_init(&block)
    end

  end


  # SUB socket.
  #
  class SUB < Socket
    include Readable

    # @return [String] subscription prefix to subscribe to everything
    #
    EVERYTHING = ''


    # @param endpoints [String, nil] endpoint to bind/connect
    # @param recv_hwm [Integer, nil] receive high water mark
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param subscribe [String, nil] subscription prefix; +nil+ (default)
    #   means no subscription — call {#subscribe} explicitly.
    # @param on_mute [Symbol] :block (default), :drop_newest, or :drop_oldest
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, recv_hwm: nil, recv_timeout: nil,
                   subscribe: nil, on_mute: :block, backend: nil, &block)
      init_engine(:SUB, recv_hwm: recv_hwm, recv_timeout: recv_timeout,
                  on_mute: on_mute, backend: backend)
      attach_endpoints(endpoints, default: :connect)
      self.subscribe(subscribe) unless subscribe.nil?
      finalize_init(&block)
    end


    # Subscribes to a topic prefix.
    #
    # @param prefix [String]
    # @return [void]
    #
    def subscribe(prefix = EVERYTHING)
      @engine.subscribe(prefix)
    end


    # Unsubscribes from a topic prefix.
    #
    # @param prefix [String]
    # @return [void]
    #
    def unsubscribe(prefix)
      @engine.unsubscribe(prefix)
    end

  end


  # XPUB socket — like PUB but exposes subscription events to the application.
  #
  class XPUB < Socket
    include Readable
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark
    # @param recv_hwm [Integer, nil] receive high water mark
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param on_mute [Symbol] mute strategy for slow subscribers
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY,
                   send_hwm: nil, recv_hwm: nil,
                   send_timeout: nil, recv_timeout: nil,
                   on_mute: :drop_newest, backend: nil, &block)
      init_engine(:XPUB, send_hwm: send_hwm, recv_hwm: recv_hwm,
                  send_timeout: send_timeout, recv_timeout: recv_timeout,
                  on_mute: on_mute, backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :bind)
      finalize_init(&block)
    end

  end


  # XSUB socket — like SUB but subscriptions are sent as data frames.
  #
  class XSUB < Socket
    include Readable
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark
    # @param recv_hwm [Integer, nil] receive high water mark
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param subscribe [String, nil] subscription prefix; +nil+ (default)
    #   means no subscription — send a subscribe frame explicitly.
    # @param on_mute [Symbol] mute strategy (:block, :drop_newest, :drop_oldest)
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY,
                   send_hwm: nil, recv_hwm: nil,
                   send_timeout: nil, recv_timeout: nil,
                   subscribe: nil, on_mute: :block, backend: nil, &block)
      init_engine(:XSUB, send_hwm: send_hwm, recv_hwm: recv_hwm,
                  send_timeout: send_timeout, recv_timeout: recv_timeout,
                  on_mute: on_mute, backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :connect)
      send("\x01#{subscribe}".b) unless subscribe.nil?
      finalize_init(&block)
    end

  end

end
