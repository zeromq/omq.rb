# frozen_string_literal: true

module OMQ
  class Engine
    # Owns the socket-level state: `:new → :open → :closing → :closed`,
    # the first-peer / last-peer signaling promises, the reconnect flag,
    # and the captured parent task for the socket's task tree.
    #
    # Scope boundary: SocketLifecycle is per-socket and outlives every
    # individual peer link. ConnectionLifecycle is per-connection and
    # handles one handshake → ready → closed arc beneath it. Roughly:
    # SocketLifecycle answers "is this socket open and do we have any
    # peers?", ConnectionLifecycle answers "is this specific peer link
    # ready / lost?".
    #
    # Engine delegates state queries here and uses it to coordinate the
    # ordering of close-time side effects. This consolidates six ivars
    # (`@state`, `@peer_connected`, `@all_peers_gone`, `@reconnect_enabled`,
    # `@parent_task`, `@on_io_thread`) into one cohesive object with
    # explicit transitions.
    #
    class SocketLifecycle
      class InvalidTransition < RuntimeError
      end


      STATES = %i[new open closing closed].freeze


      TRANSITIONS = {
        new:     %i[open closed],
        open:    %i[closing closed],
        closing: %i[closed],
        closed:  [],
      }.transform_values(&:freeze).freeze


      # @return [Symbol]
      attr_reader :state


      # @return [Async::Promise] resolves with the first connected peer
      attr_reader :peer_connected


      # @return [Async::Promise] resolves once all peers are gone (after having had peers)
      attr_reader :all_peers_gone


      # @return [Async::Task, Async::Barrier, Async::Semaphore, nil] root of
      #   the socket's task tree (may be user-provided via +parent:+ on
      #   {Socket#bind} / {Socket#connect}; falls back to the current
      #   Async task or the shared Reactor root)
      attr_reader :parent_task


      # @return [Boolean] true if parent_task is the shared Reactor thread
      attr_reader :on_io_thread


      # @return [Async::Barrier] holds every socket-scoped task (connection
      #   supervisors, reconnect loops, heartbeat, monitor, accept loops).
      #   {Engine#stop} and {Engine#close} call +barrier.stop+ to cascade
      #   teardown through every per-connection barrier in one shot.
      attr_reader :barrier


      # @return [Boolean] whether auto-reconnect is enabled
      attr_accessor :reconnect_enabled


      def initialize
        @state             = :new
        @peer_connected    = Async::Promise.new
        @all_peers_gone    = Async::Promise.new
        @reconnect_enabled = true
        @on_io_thread      = false
        @parent_task       = nil
        @barrier           = nil
      end


      def open?
        @state == :open
      end


      def closing?
        @state == :closing
      end


      def closed?
        @state == :closed
      end


      def alive?
        @state == :new || @state == :open
      end


      # Captures the socket's task tree root. Transitions `:new → :open`.
      #
      # When +parent+ is provided (any Async task/barrier/semaphore — any
      # object that responds to +#async+), it is used as the root; this is
      # the common Async idiom for letting callers place internal tasks
      # under a caller-managed parent so teardown can be coordinated with
      # other work. Otherwise falls back to the current Async task or the
      # shared Reactor root for non-Async callers.
      #
      # The socket-level {#barrier} is constructed with the captured root
      # as its parent so every task spawned via +barrier.async+ lives
      # under the caller's tree.
      #
      # @param parent [#async, nil] optional Async parent
      # @param linger [Numeric, nil] used to register the Reactor linger slot
      #   when falling back to the IO thread
      # @return [Boolean] true on first-time capture, false if already captured
      #
      def capture_parent_task(parent: nil, linger:)
        return false if @parent_task

        if parent
          @parent_task  = parent
        elsif Async::Task.current?
          @parent_task = Async::Task.current
        else
          @parent_task  = Reactor.root_task
          @on_io_thread = true
          Reactor.track_linger(linger)
        end

        @barrier = Async::Barrier.new(parent: @parent_task)
        transition!(:open)
        true
      end


      # Transitions `:open → :closing`.
      def start_closing!
        transition!(:closing)
      end


      # Transitions `:closing → :closed` (or `:new → :closed` for
      # never-opened sockets).
      def finish_closing!
        transition!(:closed)
      end


      # Hard close from any alive state — used by {Engine#stop}'s crash
      # path, which skips the linger drain. Goes `:open → :closed`
      # directly when needed.
      def force_close!
        transition!(:closed)
      end


      # Resolves `all_peers_gone` once we had peers and the connection map
      # is empty. Called by {Engine#maybe_resolve_all_peers_gone} after a
      # teardown removes a peer.
      #
      # @param connections [Hash] current connection map
      def maybe_resolve_all_peers_gone(connections)
        return unless @peer_connected.resolved?
        return unless connections.empty?

        @all_peers_gone.resolve(true)
      end


      private


      def transition!(new_state)
        allowed = TRANSITIONS[@state]

        unless allowed&.include?(new_state)
          raise InvalidTransition, "#{@state} → #{new_state}"
        end

        @state = new_state
      end

    end
  end
end
