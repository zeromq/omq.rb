# frozen_string_literal: true

module OMQ
  class Engine
    # Recv pump for a connection.
    #
    # For inproc Pipe: wires the direct recv path (no fiber spawned).
    # For TCP/IPC: spawns a transient task that reads messages from the
    # connection and enqueues them into +recv_queue+.
    #
    # The two-method structure (with/without transform) is intentional for
    # YJIT: it gives the JIT a monomorphic call per routing strategy instead
    # of a megamorphic `transform.call` dispatch inside a shared loop.
    #
    class RecvPump
      # Max messages read from one connection before yielding to the
      # scheduler. Prevents a busy peer from starving its siblings in
      # fair-queue recv sockets. Symmetric with RoundRobin send batching.
      FAIRNESS_MESSAGES = 256


      # Max bytes read from one connection before yielding. Only counted
      # for ZMTP connections (inproc skips the check). Complements
      # {FAIRNESS_MESSAGES}: small-message floods are bounded by count,
      # large-message floods by bytes. Symmetric with RoundRobin send batching.
      FAIRNESS_BYTES    = 512 * 1024


      # Public entry point — callers use the class method.
      #
      # @param parent [Async::Task, Async::Barrier] parent to spawn under
      # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
      # @param recv_queue [Async::LimitedQueue]
      # @param engine [Engine]
      # @param transform [Proc, nil]
      # @return [Async::Task, nil]
      #
      def self.start(parent, conn, recv_queue, engine, transform)
        new(conn, recv_queue, engine).start(parent, transform)
      end


      # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
      # @param recv_queue [Async::LimitedQueue]
      # @param engine [Engine]
      #
      def initialize(conn, recv_queue, engine)
        @conn        = conn
        @recv_queue  = recv_queue
        @engine      = engine
        @count_bytes = conn.instance_of?(Protocol::ZMTP::Connection)
      end


      # Starts the recv pump. For inproc Pipe, wires the direct path
      # (no task spawned). For TCP/IPC, spawns a fiber that reads messages.
      #
      # @param parent_task [Async::Task]
      # @param transform [Proc, nil] optional per-message transform
      # @return [Async::Task, nil]
      #
      def start(parent_task, transform)
        if @conn.is_a?(Transport::Inproc::Pipe) && @conn.peer
          @conn.peer.wire_direct_recv(@recv_queue, transform)
          return nil
        end

        if transform
          start_with_transform(parent_task, transform)
        else
          start_direct(parent_task)
        end
      end


      private


      # Recv loop with per-message transform (e.g. Marshal.load for
      # cross-Ractor transport). Kept separate from {#start_direct} so
      # YJIT sees a monomorphic transform.call site.
      #
      # A transform that returns +nil+ drops the message — the recv pump
      # still counts it toward fairness (so dup-floods can't starve
      # siblings) but it is neither emitted nor enqueued to the
      # application.
      #
      # @param parent [Async::Task, Async::Barrier]
      # @param transform [Proc]
      # @return [Async::Task]
      #
      def start_with_transform(parent, transform)
        conn, recv_queue, engine, count_bytes = @conn, @recv_queue, @engine, @count_bytes

        parent.async(transient: true, annotation: "recv pump") do |task|
          loop do
            count = 0
            bytes = 0

            while count < FAIRNESS_MESSAGES && bytes < FAIRNESS_BYTES
              msg = conn.receive_message
              msg.each { |part| part.freeze }
              msg.freeze

              # hot path bytes — count before transform so dropped
              # messages still contribute to the fairness cap.
              if count_bytes
                if msg.size == 1
                  bytes += msg.first.bytesize
                else
                  i, n = 0, msg.size
                  while i < n
                    bytes += msg[i].bytesize
                    i += 1
                  end
                end
              end

              count += 1
              transformed = transform.call(msg)
              next unless transformed

              # Emit the verbose trace BEFORE enqueueing so the monitor
              # fiber is woken before the application fiber -- the
              # async scheduler is FIFO on the ready list, so this
              # preserves log-before-stdout ordering for -vvv traces.
              engine.emit_verbose_msg_received(conn, transformed)
              recv_queue.enqueue(transformed)
            end

            task.yield
          end
        rescue Async::Stop, Async::Cancel
        rescue Protocol::ZMTP::Error, *CONNECTION_LOST => error
          # expected disconnect — stash reason for the :disconnected
          # monitor event, let the lifecycle reconnect as usual
          engine.record_disconnect_reason(conn, error)
        rescue => error
          engine.signal_fatal_error(error)
        end
      end


      # Recv loop without transform — the hot path for native OMQ use.
      #
      # @param parent [Async::Task, Async::Barrier]
      # @return [Async::Task]
      #
      def start_direct(parent)
        conn, recv_queue, engine, count_bytes = @conn, @recv_queue, @engine, @count_bytes

        parent.async(transient: true, annotation: "recv pump") do |task|
          loop do
            count = 0
            bytes = 0

            while count < FAIRNESS_MESSAGES && bytes < FAIRNESS_BYTES
              msg = conn.receive_message
              msg.each { |part| part.freeze }
              msg.freeze
              engine.emit_verbose_msg_received(conn, msg)
              recv_queue.enqueue(msg)

              count += 1

              # hot path
              if count_bytes
                if msg.size == 1
                  bytes += msg.first.bytesize
                else
                  i, n = 0, msg.size
                  while i < n
                    bytes += msg[i].bytesize
                    i += 1
                  end
                end
              end
            end

            task.yield
          end
        rescue Async::Stop, Async::Cancel
        rescue Protocol::ZMTP::Error, *CONNECTION_LOST => error
          # expected disconnect — stash reason for the :disconnected
          # monitor event, let the lifecycle reconnect as usual
          engine.record_disconnect_reason(conn, error)
        rescue => error
          engine.signal_fatal_error(error)
        end
      end

    end
  end
end
