# frozen_string_literal: true

require "async/promise"

module OMQ
  class QoS
    # Adds the +qos+ accessors to {OMQ::Socket} and validates +qos=+
    # against the RFC. Fan-out socket types MUST refuse any QoS level
    # above 0, and Integer levels are rejected in favour of {OMQ::QoS}
    # instances (a 0.x → 0.3 break — see CHANGELOG).
    #
    module SocketExt
      FAN_OUT_TYPES = %i[PUB XPUB RADIO SUB XSUB DISH].freeze


      def qos
        @options.qos
      end


      def qos=(value)
        unless value.nil? || value.is_a?(OMQ::QoS)
          raise ArgumentError,
                "QoS must be nil (fire-and-forget) or an OMQ::QoS instance " \
                "(OMQ::QoS.at_least_once / .exactly_once / .exactly_once_and_processed); " \
                "received #{value.inspect}"
        end

        if value && FAN_OUT_TYPES.include?(@engine.socket_type)
          raise ArgumentError,
                "QoS > 0 is not supported for fan-out socket type #{@engine.socket_type}; " \
                "use a broker (e.g. Malamute) for reliable fan-out"
        end

        current = @options.qos
        if current && !current.equal?(value)
          raise ArgumentError,
                "OMQ::QoS already attached to this socket; a different instance " \
                "cannot be assigned (reset to nil first if supported)"
        end

        @options.qos = value
        value&.attach!(@engine)
      end


      def close
        @options.qos&.shutdown
        super
      end
    end


    # Prepended onto {OMQ::Readable} so that at QoS 3 +#receive+ accepts
    # (and requires) a block. The block runs the application handler;
    # its return value becomes a COMP command; a raised {StandardError}
    # becomes a NACK with the mapped error code.
    #
    # At QoS 0/1/2 +#receive+ is unchanged.
    #
    module ReadableExt
      def receive(&block)
        qos = @options.qos
        return super() if qos.nil? || qos.level < 3

        raise ArgumentError, "QoS 3 requires a block to #receive — the block's return or raise signals COMP/NACK" unless block

        env = super()
        QoS.process_qos3_envelope(env, qos, &block)
      end


      def each(&block)
        qos = @options.qos
        return super unless qos && qos.level >= 3

        raise ArgumentError, "QoS 3 requires a block to #each" unless block

        loop do
          receive(&block)
        rescue IO::TimeoutError
          return
        end
      end
    end


    # Runs the application block against a QoS 3 {Envelope} and emits
    # COMP on success, NACK on any +StandardError+.
    #
    # Success path: add the digest to the peer's dedup set first, then
    # send COMP — a late retransmit from the sender (crossing our COMP
    # on the wire) must see a seen-digest so the receiver answers with a
    # duplicate COMP rather than invoking the handler again.
    #
    # @param env [Envelope]
    # @param qos [OMQ::QoS]
    # @yieldparam parts [Array<String>]
    # @return [Array<String>] the original message parts
    def self.process_qos3_envelope(env, qos, &block)
      parts  = env.parts
      conn   = env.conn
      digest = env.digest
      algo   = env.algo
      dedup  = qos.dedup_set_for(conn)

      if (timeout = qos.processing_timeout)
        Async::Task.current.with_timeout(timeout, TimeoutError) do
          block.call(parts)
        end
      else
        block.call(parts)
      end

      dedup.add(digest)
      conn.send_command(Protocol::ZMTP::Codec::Command.comp(digest, algorithm: algo))
      parts
    rescue StandardError => error
      code, msg = ErrorCodes.exception_to_payload(error)
      conn.send_command(Protocol::ZMTP::Codec::Command.nack(digest, code: code, message: msg, algorithm: algo))
      parts
    end


    # Prepended onto {OMQ::Writable} so that at QoS >= 2 +#send+ returns
    # an {Async::Promise} resolving to +:delivered+ or a {DeadLetter}.
    # At QoS 0/1 the original {Writable#send} is preserved (returns
    # +self+).
    #
    # The Promise is stashed on the socket's {OMQ::QoS} via
    # {QoS#enqueue_promise} keyed by +parts.object_id+. The routing
    # layer ({RoundRobinExt#write_batch}) takes it back out when the
    # message reaches the wire and attaches it to the
    # {PeerRegistry::Entry}. This keeps the send queue's item type
    # uniform (Array<String>) across QoS levels, so +batch_bytes+,
    # +emit_verbose_msg_sent+, and {RecvPump} need no awareness of QoS.
    #
    module WritableExt
      def send(message)
        qos = @options.qos
        return super if qos.nil? || qos.level < 2

        parts = message.is_a?(Array) ? message : [message]
        raise ArgumentError, "message has no parts" if parts.empty?

        parts = parts.map { it.to_str } if parts.any? { !it.is_a?(String) }
        parts.each do |part|
          part.force_encoding(Encoding::BINARY) unless part.frozen? || part.encoding == Encoding::BINARY
          part.freeze
        end
        parts.freeze

        promise = Async::Promise.new
        qos.enqueue_promise(parts, promise)

        if @engine.on_io_thread?
          OMQ::Reactor.run(timeout: @options.write_timeout) { @engine.enqueue_send(parts) }
        elsif (timeout = @options.write_timeout)
          Async::Task.current.with_timeout(timeout, IO::TimeoutError) { @engine.enqueue_send(parts) }
        else
          @engine.enqueue_send(parts)
        end

        promise
      end
    end
  end
end


OMQ::Socket.prepend(OMQ::QoS::SocketExt)
OMQ::Writable.prepend(OMQ::QoS::WritableExt)
OMQ::Readable.prepend(OMQ::QoS::ReadableExt)
