# frozen_string_literal: true

module OMQ
  class QoS
    # Prepended onto OMQ::Engine::ConnectionLifecycle so that the
    # Protocol::ZMTP::Connection is built with this socket's QoS
    # metadata, and the peer's QoS properties are validated after the
    # handshake.
    #
    module LifecycleExt
      def handshake!(io, as_server:)
        transition!(:handshaking)
        conn = Protocol::ZMTP::Connection.new io,
          socket_type:      @engine.socket_type.to_s,
          identity:         @engine.options.identity,
          as_server:        as_server,
          mechanism:        @engine.options.mechanism&.dup,
          max_message_size: @engine.options.max_message_size,
          **QoS.handshake_metadata(@engine.options)

        Async::Task.current.with_timeout(handshake_timeout) do
          conn.handshake!
        end

        QoS.validate_handshake!(@engine.options, conn)

        OMQ::Engine::Heartbeat.start(@barrier, conn, @engine.options)
        ready!(conn)
        @conn
      rescue Protocol::ZMTP::Error, *OMQ::CONNECTION_LOST, Async::TimeoutError => error
        @engine.emit_monitor_event :handshake_failed,
          endpoint: @endpoint, detail: { error: error }

        conn&.close

        tear_down!(reconnect: true)
        raise
      end

    end


    # Builds the READY-property metadata hash this socket should
    # advertise based on its QoS configuration. Empty hash at QoS 0 so
    # the Connection sees no extras.
    #
    # @param options [OMQ::Options]
    # @return [Hash{String => String}]
    def self.handshake_metadata(options)
      qos = options.qos
      return {} if qos.nil?
      meta = { "X-QoS" => qos.level.to_s }
      meta["X-QoS-Hash"] = qos.hash_algos unless qos.hash_algos.empty?
      meta
    end


    # Validates the peer's READY properties against the local QoS
    # configuration. At QoS >= 2 also asserts that the connection has a
    # stable per-peer identity (CURVE pubkey or non-empty ZMQ_IDENTITY)
    # — without one the {PeerRegistry} cannot pin pending messages
    # across reconnects.
    def self.validate_handshake!(options, conn)
      props     = conn.peer_properties || {}
      qos       = options.qos
      local_lvl = qos&.level || 0
      peer_lvl  = (props["X-QoS"] || "0").to_i

      if local_lvl != peer_lvl
        raise Protocol::ZMTP::Error,
              "QoS mismatch: local=#{local_lvl} peer=#{peer_lvl}"
      end

      return if local_lvl == 0

      local_hash = qos.hash_algos
      peer_hash  = props["X-QoS-Hash"] || ""

      unless local_hash.empty? || peer_hash.empty?
        algo = local_hash.each_char.find { |c| peer_hash.include?(c) }
        unless algo
          raise Protocol::ZMTP::Error,
                "QoS hash algorithm mismatch: local=#{local_hash.inspect} peer=#{peer_hash.inspect}"
        end
      end

      return if local_lvl < 2

      info = conn.peer_info
      if info.nil? || (info.public_key.nil? && info.identity.empty?)
        raise Protocol::ZMTP::Error,
              "QoS #{local_lvl} requires a stable peer anchor " \
              "(CURVE public key or non-empty ZMQ_IDENTITY)"
      end
    end
  end
end


OMQ::Engine::ConnectionLifecycle.prepend(OMQ::QoS::LifecycleExt)
