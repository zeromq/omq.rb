# frozen_string_literal: true

module OMQ
  # PUSH socket — push messages to connected PULL peers via round-robin.
  #
  class PUSH < Socket
    include Writable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param linger [Numeric] linger period in seconds (Float::INFINITY = wait forever, 0 = drop)
    # @param send_hwm [Integer, nil] send high water mark (nil uses default)
    # @param send_timeout [Numeric, nil] send timeout in seconds
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, linger: Float::INFINITY, send_hwm: nil, send_timeout: nil, backend: nil, &block)
      init_engine(:PUSH, send_hwm: send_hwm, send_timeout: send_timeout, backend: backend)
      @options.linger = linger
      attach_endpoints(endpoints, default: :connect)
      finalize_init(&block)
    end

  end


  # PULL socket — receive messages from PUSH peers via fair-queue.
  #
  class PULL < Socket
    include Readable

    # @param endpoints [String, nil] endpoint to bind/connect
    # @param recv_hwm [Integer, nil] receive high water mark (nil uses default)
    # @param recv_timeout [Numeric, nil] receive timeout in seconds
    # @param backend [Symbol, nil] registered backend name (:ruby by default)
    #
    def initialize(endpoints = nil, recv_hwm: nil, recv_timeout: nil, backend: nil, &block)
      init_engine(:PULL, recv_hwm: recv_hwm, recv_timeout: recv_timeout, backend: backend)
      attach_endpoints(endpoints, default: :bind)
      finalize_init(&block)
    end

  end

end
