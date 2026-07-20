# frozen_string_literal: true

require "async/clock"

module OMQ
  class QoS
    # Installs the QoS 1 command handler: dispatches incoming ACK frames
    # to +pending_store.ack+.
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @param pending_store [PendingStore]
    def self.install_qos1_handler(conn, pending_store)
      conn.qos_on_command = lambda do |cmd|
        next unless cmd.name == "ACK"
        _, hash = cmd.ack_data
        pending_store.ack(hash)
      end
    end


    # Installs the QoS 2 sender-side command handler on +conn+.
    # Dispatches ACK → resolve promise + send CLR + release a
    # {OMQ::QoS#send_semaphore} permit.
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @param qos [OMQ::QoS]
    # @param algo [String]
    def self.install_qos2_sender_handler(conn, qos, algo)
      peer_info = conn.peer_info
      semaphore = qos.send_semaphore
      registry  = qos.peer_registry
      clr_class = Protocol::ZMTP::Codec::Command

      conn.qos_on_command = lambda do |cmd|
        next unless cmd.name == "ACK"
        _, hash = cmd.ack_data
        entry = registry.ack(peer_info, hash)
        next unless entry

        entry.promise.resolve(:delivered) unless entry.promise.resolved?
        semaphore.release

        conn.send_command(clr_class.clr(hash, algorithm: algo))
      end
    end


    # Installs the QoS 3 sender-side command handler on +conn+.
    # Dispatches COMP → resolve promise + send CLR + release a permit;
    # NACK → classify code, either dead-letter terminally or schedule a
    # retry with exponential backoff (up to +max_retries+).
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @param qos [OMQ::QoS]
    # @param algo [String]
    def self.install_qos3_sender_handler(conn, qos, algo)
      peer_info = conn.peer_info
      semaphore = qos.send_semaphore
      registry  = qos.peer_registry
      cmd_class = Protocol::ZMTP::Codec::Command

      conn.qos_on_command = lambda do |cmd|
        case cmd.name
        when "COMP"
          _, hash = cmd.comp_data
          entry = registry.ack(peer_info, hash)
          next unless entry

          entry.promise.resolve(:delivered) unless entry.promise.resolved?
          semaphore.release
          conn.send_command(cmd_class.clr(hash, algorithm: algo))

        when "NACK"
          _, hash, code, message = cmd.nack_data
          entry = registry.ack(peer_info, hash)
          next unless entry

          handle_qos3_nack(conn, qos, algo, peer_info, entry, hash, code, message)
        end
      end
    end


    # NACK dispatch for QoS 3 senders. Terminal codes dead-letter
    # immediately; retryable codes either retry on the same peer after
    # exponential backoff or dead-letter once +max_retries+ is reached.
    #
    # @return [void]
    def self.handle_qos3_nack(conn, qos, algo, peer_info, entry, hash, code, message)
      nack_info = NackInfo.new(code: code, message: message)

      unless ErrorCodes.retryable?(code)
        QoS.dead_letter(entry,
          peer_info: peer_info,
          reason:    :terminal_nack,
          semaphore: qos.send_semaphore,
          error:     nack_info,
        )
        return
      end

      if entry.retry_count + 1 >= qos.max_retries
        QoS.dead_letter(entry,
          peer_info: peer_info,
          reason:    :retry_exhausted,
          semaphore: qos.send_semaphore,
          error:     nack_info,
        )
        return
      end

      next_entry = entry.with(retry_count: entry.retry_count + 1, sent_at: Async::Clock.now)
      qos.peer_registry.track(peer_info, hash, next_entry, conn)

      delay = RetryScheduler.delay(entry.retry_count, qos.retry_backoff)
      qos.spawn_task(annotation: "qos3 retry") do
        sleep delay
        live_conn = qos.peer_registry.connection_for(peer_info)
        live_conn&.send_message(next_entry.parts)
      end
    end


    # Installs the QoS 2 receiver-side command handler on +conn+.
    # Dispatches CLR → evict digest from the peer's dedup set.
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @param dedup [DedupSet]
    def self.install_qos2_receiver_handler(conn, dedup)
      conn.qos_on_command = lambda do |cmd|
        next unless cmd.name == "CLR"
        _, hash = cmd.clr_data
        dedup.remove(hash)
      end
    end


    # Tuple enqueued into the recv queue at QoS 3 so that
    # {SocketExt#receive} can send the appropriate COMP/NACK on the
    # source +conn+ after the application block runs.
    Envelope = Data.define(:parts, :conn, :digest, :algo)


    # Inproc Pipes deliver messages synchronously through a shared
    # in-memory queue, so there's nothing for at-least-once to protect
    # against. QoS hooks short-circuit when they see a Pipe.
    #
    # @param conn [Object]
    # @return [Boolean]
    #
    def self.reliable_transport?(conn)
      defined?(OMQ::Transport::Inproc::Pipe) &&
        conn.is_a?(OMQ::Transport::Inproc::Pipe)
    end


    # Picks a hash algorithm for a connection by intersecting our
    # preferences with the peer's advertised list (read from the peer's
    # READY properties).
    #
    # @param connection [Protocol::ZMTP::Connection]
    # @return [String] single-char algorithm identifier
    #
    def self.algo_for(connection)
      peer = connection.respond_to?(:peer_properties) ? connection.peer_properties : nil
      negotiate_hash(peer&.fetch("X-QoS-Hash", "") || "") || DEFAULT_HASH_ALGO
    end


    # Prepended onto {Routing::RoundRobin}. Branches by QoS level:
    #
    #   0 — no-op, defer entirely to base.
    #   1 — existing {PendingStore} path. Disconnect drains entries
    #       back into the send queue (failover).
    #   2 — {PeerRegistry} path. Each outgoing message carries an
    #       {Async::Promise} (via {OMQ::QoS#take_enqueue_promise}); the
    #       registry pins it to the peer's {Protocol::ZMTP::PeerInfo}
    #       and a connection-drop leaves entries in place for replay
    #       on reconnect (or dead-letter after the timeout).
    #
    module RoundRobinExt
      private


      def init_round_robin(engine)
        super
        @pending_store  = nil
        @conn_algos     = {}
        @conn_peer_info = {}
      end


      # Lazily built QoS-1 pending store. Nil at QoS 0 and QoS >= 2
      # (those paths use {OMQ::QoS#peer_registry} instead).
      def pending_store
        return @pending_store if @pending_store
        qos = @engine.options.qos
        @pending_store = PendingStore.new(capacity: @engine.options.send_hwm) if qos&.level == 1
        @pending_store
      end


      def algo_for(conn)
        @conn_algos[conn] ||= QoS.algo_for(conn)
      end


      def peer_info_for(conn)
        @conn_peer_info[conn] ||= conn.peer_info
      end


      def remove_round_robin_send_connection(conn)
        super
        qos = @engine.options.qos

        if qos && qos.level >= 2 && !QoS.reliable_transport?(conn)
          peer_info = @conn_peer_info.delete(conn)
          qos.peer_registry.disconnect(peer_info) if peer_info
          @conn_algos.delete(conn)
        elsif (ps = @pending_store)
          ps.messages_for(conn).each { |entry| @send_queue.enqueue(entry.parts) }
          @conn_algos.delete(conn)
        end
      end


      def write_batch(conn, batch)
        qos = @engine.options.qos

        if qos && qos.level >= 2 && !QoS.reliable_transport?(conn)
          write_batch_qos2(conn, batch, qos)
        elsif (ps = @pending_store) && !QoS.reliable_transport?(conn)
          super
          algo = algo_for(conn)
          batch.each do |parts|
            ps.wait_for_slot
            wire_parts = transform_send(parts)
            ps.track(QoS.digest(wire_parts, algorithm: algo), parts, conn)
          end
        else
          super
        end
      end


      def write_batch_qos2(conn, batch, qos)
        peer_info = peer_info_for(conn)
        algo      = algo_for(conn)
        registry  = qos.peer_registry
        semaphore = qos.send_semaphore

        batch.each do |parts|
          promise = qos.take_enqueue_promise(parts) or
            raise "OMQ::QoS #{qos.level}: missing enqueue Promise for outgoing parts"

          semaphore.acquire

          wire_parts = transform_send(parts)
          conn.send_message(wire_parts)

          digest = QoS.digest(wire_parts, algorithm: algo)
          entry  = PeerRegistry::Entry.new(
            parts:       parts,
            peer_info:   peer_info,
            sent_at:     Async::Clock.now,
            promise:     promise,
            retry_count: 0,
          )
          registry.track(peer_info, digest, entry, conn)
        end
      end
    end


    # Shared helper for the send-side connection_added path: installs
    # the QoS 1 or QoS 2 command handler, and at QoS 2 replays any
    # pending entries for the peer onto the fresh connection.
    #
    # @param socket [OMQ::Routing::RoundRobin] the routing strategy instance
    # @param conn [Protocol::ZMTP::Connection]
    # @return [void]
    def self.on_send_connection_added(socket, conn)
      qos = socket.instance_variable_get(:@engine).options.qos
      return if qos.nil?
      return if reliable_transport?(conn)

      algo = socket.send(:algo_for, conn)

      case qos.level
      when 1
        install_qos1_handler(conn, socket.send(:pending_store))
      when 2
        install_qos2_sender_handler(conn, qos, algo)
        qos.ensure_sweep_task_started
        replay_pending(conn, qos)
      when 3
        install_qos3_sender_handler(conn, qos, algo)
        qos.ensure_sweep_task_started
        replay_pending(conn, qos)
      end
    end


    # Replays any pending entries for +conn.peer_info+ on +conn+,
    # preserving original insertion order. Called on reconnect at
    # QoS >= 2.
    #
    # @param conn [Protocol::ZMTP::Connection]
    # @param qos [OMQ::QoS]
    def self.replay_pending(conn, qos)
      peer_info = conn.peer_info
      entries   = qos.peer_registry.resume(peer_info, conn)
      return if entries.empty?

      entries.each do |entry|
        conn.send_message(entry.parts)
      end
    end


    # Prepended onto {Routing::Push}. Installs the per-level command
    # handler on every new connection. The existing reaper fiber keeps
    # blocking on {Connection#receive_message}, which dispatches
    # incoming command frames through {ConnectionExt} to our handler.
    #
    module PushExt
      def connection_added(conn)
        super
        QoS.on_send_connection_added(self, conn)
      end
    end


    # Same pattern as PushExt — SCATTER uses RoundRobin too.
    #
    module ScatterExt
      def connection_added(conn)
        super
        QoS.on_send_connection_added(self, conn)
      end
    end


    # Prepended onto {Routing::Pull}. At QoS >= 1 every received message
    # is ACK'd back to the sender. At QoS >= 2 the ACK is paired with a
    # dedup-set check: seen digests are ACK'd again but not redelivered
    # to the application.
    #
    # ACK after successful enqueue into +recv_queue+. A receiver that
    # has read a frame off the wire but not yet stored it (because the
    # app-facing recv_queue is at +recv_hwm+) has NOT yet taken
    # responsibility for it — if we ACK'd on wire-receipt and then
    # crashed before the app dequeued, the sender would have cleared
    # pending on a message that was effectively lost. ACK-after-enqueue
    # keeps the at-least-once contract honest and lets +recv_hwm+
    # bound truly-in-flight (= read-but-not-stored) messages at zero.
    #
    # The transform returns +nil+ so the pump's own post-transform
    # enqueue is skipped — we already enqueued inside the transform.
    module PullExt
      def connection_added(conn)
        qos = @engine.options.qos
        return super if qos.nil?
        return super if QoS.reliable_transport?(conn)

        algo       = QoS.algo_for(conn)
        recv_queue = @recv_queue
        engine     = @engine

        case qos.level
        when 1
          engine.start_recv_pump(conn, recv_queue) do |msg|
            recv_queue.enqueue(msg)
            conn.send_command(QoS.ack_command(msg, algorithm: algo))
            nil
          end

        when 2
          dedup = qos.dedup_set_for(conn)
          QoS.install_qos2_receiver_handler(conn, dedup)

          engine.start_recv_pump(conn, recv_queue) do |msg|
            digest = QoS.digest(msg, algorithm: algo)
            ack    = Protocol::ZMTP::Codec::Command.ack(digest, algorithm: algo)

            if dedup.seen?(digest)
              conn.send_command(ack)
            else
              dedup.add(digest)
              recv_queue.enqueue(msg)
              conn.send_command(ack)
            end
            nil
          end

        when 3
          dedup = qos.dedup_set_for(conn)
          QoS.install_qos2_receiver_handler(conn, dedup)

          engine.start_recv_pump(conn, recv_queue) do |msg|
            digest = QoS.digest(msg, algorithm: algo)
            if dedup.seen?(digest)
              conn.send_command(Protocol::ZMTP::Codec::Command.comp(digest, algorithm: algo))
            else
              recv_queue.enqueue(QoS::Envelope.new(parts: msg, conn: conn, digest: digest, algo: algo))
            end
            nil
          end
        end
      end
    end


    # Same pattern as PullExt — GATHER uses fair-recv too.
    #
    module GatherExt
      def connection_added(conn)
        qos = @engine.options.qos
        return super if qos.nil?
        return super if QoS.reliable_transport?(conn)

        algo       = QoS.algo_for(conn)
        recv_queue = @recv_queue
        engine     = @engine

        case qos.level
        when 1
          engine.start_recv_pump(conn, recv_queue) do |msg|
            recv_queue.enqueue(msg)
            conn.send_command(QoS.ack_command(msg, algorithm: algo))
            nil
          end

        when 2
          dedup = qos.dedup_set_for(conn)
          QoS.install_qos2_receiver_handler(conn, dedup)

          engine.start_recv_pump(conn, recv_queue) do |msg|
            digest = QoS.digest(msg, algorithm: algo)
            ack    = Protocol::ZMTP::Codec::Command.ack(digest, algorithm: algo)

            if dedup.seen?(digest)
              conn.send_command(ack)
            else
              dedup.add(digest)
              recv_queue.enqueue(msg)
              conn.send_command(ack)
            end
            nil
          end

        when 3
          dedup = qos.dedup_set_for(conn)
          QoS.install_qos2_receiver_handler(conn, dedup)

          engine.start_recv_pump(conn, recv_queue) do |msg|
            digest = QoS.digest(msg, algorithm: algo)
            if dedup.seen?(digest)
              conn.send_command(Protocol::ZMTP::Codec::Command.comp(digest, algorithm: algo))
            else
              recv_queue.enqueue(QoS::Envelope.new(parts: msg, conn: conn, digest: digest, algo: algo))
            end
            nil
          end
        end
      end
    end


    # Routing::Req needs no QoS-specific override at levels 0/1:
    # re-enqueueing a mid-flight request on connection loss is already
    # handled by {RoundRobinExt#remove_round_robin_send_connection}
    # through the pending store. At QoS >= 2 the request is instead
    # pinned in {PeerRegistry}; the RoundRobinExt path handles that
    # directly, and REQ's +@state+ legitimately stays +:waiting_reply+
    # until either the pinned peer returns with the reply or the
    # dead-letter timer fires (which the application observes through
    # the +#send+ Promise).
  end
end
