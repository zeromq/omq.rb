# frozen_string_literal: true

module OMQ
  class Engine
    # Spawning pump tasks, and turning an unexpected pump error into
    # a dead socket.
    #
    module PumpTasks
      # Spawns a transient pump task with error propagation.
      #
      # Unexpected exceptions are caught and forwarded to
      # {#signal_fatal_error} so blocked callers (send/recv)
      # see the real error instead of deadlocking.
      #
      # @param annotation [String] task annotation for debugging
      # @param parent [Async::Task, Async::Barrier] parent for the spawned
      #   task. Defaults to the current task. Routing strategies that own
      #   loose pump tasks (Radio, Channel) pass their own +Async::Barrier+
      #   so a single +barrier.stop+ tears the lot down at strategy
      #   shutdown without per-task bookkeeping.
      # @yield the pump loop body
      # @return [Async::Task]
      #
      def spawn_pump_task(annotation:, parent: Async::Task.current, &block)
        parent.async(transient: true, annotation: annotation) do
          yield
        rescue Async::Stop, Async::Cancel, Protocol::ZMTP::Error, *CONNECTION_LOST
          # normal shutdown / expected disconnect
        rescue => error
          signal_fatal_error(error)
        end
      end


      # Spawns a per-connection pump task on the connection's own
      # lifecycle barrier. When any pump on the barrier exits (e.g. the
      # send pump sees EPIPE and calls {#connection_lost}), {ConnectionLifecycle#tear_down!}
      # calls `barrier.stop` which cancels every sibling pump for that
      # connection — so a dead peer can no longer leave orphan send
      # pumps blocked on `dequeue` waiting for messages that will never
      # be written.
      #
      # @param conn [Protocol::ZMTP::Connection, Transport::Inproc::Pipe]
      # @param annotation [String]
      #
      def spawn_conn_pump_task(conn, annotation:, &block)
        lifecycle = @connections[conn]

        unless lifecycle
          return spawn_pump_task(annotation: annotation, &block)
        end

        lifecycle.barrier.async(transient: true, annotation: annotation) do
          yield
        rescue Async::Stop, Async::Cancel
          # normal shutdown / sibling tore us down
        rescue Protocol::ZMTP::Error, *CONNECTION_LOST => error
          # expected disconnect — stash reason for the :disconnected
          # monitor event, then let the lifecycle reconnect as usual
          lifecycle.record_disconnect_reason(error)
        rescue => error
          signal_fatal_error(error)
        end
      end


      # Wraps an unexpected pump error as {OMQ::SocketDeadError} and
      # unblocks any callers waiting on the recv queue. The original
      # error is preserved as +#cause+ so callers can surface the real
      # reason.
      #
      # @param error [Exception]
      #
      def signal_fatal_error(error)
        return unless @lifecycle.open?

        @fatal_error = build_fatal_error(error)
        routing.unblock_recv rescue nil
        @lifecycle.peer_connected.resolve(nil) rescue nil
      end


      private


      # Constructs a SocketDeadError whose +cause+ is +error+. Uses the
      # raise-in-rescue idiom because Ruby only sets +cause+ on an
      # exception when it is raised from inside a rescue block -- works
      # regardless of the original caller's +$!+ state.
      def build_fatal_error(error)
        raise error
      rescue
        begin
          raise SocketDeadError, "#{@socket_type} socket killed: #{error.message}"
        rescue SocketDeadError => wrapped
          wrapped
        end
      end

    end
  end
end
