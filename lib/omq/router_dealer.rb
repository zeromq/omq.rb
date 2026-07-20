# frozen_string_literal: true

module OMQ
  # DEALER socket — asynchronous round-robin send, fair-queue receive.
  #
  class DEALER < Socket
    include Readable
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark
    # @param recv_hwm [Integer, nil] receive high water mark
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY,
                   send_hwm: nil, recv_hwm: nil,
                   send_timeout: nil, recv_timeout: nil,
                   backend: nil, &block)
      init_engine(:DEALER, send_hwm: send_hwm, recv_hwm: recv_hwm,
                  send_timeout: send_timeout, recv_timeout: recv_timeout,
                  backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :connect)
      finalize_init(&block)
    end

  end


  # ROUTER socket.
  #
  class ROUTER < Socket
    include Readable
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark
    # @param recv_hwm [Integer, nil] receive high water mark
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY,
                   send_hwm: nil, recv_hwm: nil,
                   send_timeout: nil, recv_timeout: nil,
                   backend: nil, &block)
      init_engine(:ROUTER, send_hwm: send_hwm, recv_hwm: recv_hwm,
                  send_timeout: send_timeout, recv_timeout: recv_timeout,
                  backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :bind)
      finalize_init(&block)
    end


    # Sends a message to a specific peer by identity.
    #
    # @param receiver [String] peer identity
    # @param message [String, Array<String>]
    # @return [self]
    #
    def send_to(receiver, message)
      parts = message.is_a?(Array) ? message : [message]
      send([receiver, '', *parts])
    end

  end

end
