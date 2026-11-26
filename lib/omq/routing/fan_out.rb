# frozen_string_literal: true

module OMQ
  module Routing
    # Mixin for routing strategies that fan-out to subscribers.
    #
    # Manages per-connection subscription sets, subscription command
    # listeners, and per-connection send queues/pumps that deliver
    # to each matching peer independently.
    #
    # HWM is enforced per subscriber: each connection gets its own
    # bounded send queue. DropQueues (for :drop_newest/:drop_oldest)
    # silently drop messages for a slow subscriber without affecting
    # others. LimitedQueues (for :block) block the publisher.
    #
    # Including classes must call `init_fan_out(engine)` from
    # their #initialize.
    #
    module FanOut
      # Shared frozen empty binary string to avoid repeated allocations.
      EMPTY_BINARY = ::Protocol::ZMTP::Codec::EMPTY_BINARY


      # @return [Async::Promise] resolves when the first subscriber joins
      #
      attr_reader :subscriber_joined


      # @return [Boolean] true when all per-connection send queues are empty
      #
      def send_queues_drained?
        @conn_queues.values.all?(&:empty?)
      end


      private


      def init_fan_out(engine)
        @connections        = Set.new
        @subscriptions      = {} # connection => Set of prefixes
        @subscribe_all      = Set.new # connections subscribed to "" (match-all fast path)
        @conn_queues        = {} # connection => per-connection send queue
        @fan_out_queues     = [].freeze # snapshot of @conn_queues values
        @conflate           = engine.options.conflate
        @subscriber_joined  = Async::Promise.new
        @latest             = {} if @conflate
      end


      # @return [Boolean] whether the connection is subscribed to the topic
      #
      def subscribed?(conn, topic)
        return true if @subscribe_all.include?(conn)
        subs = @subscriptions[conn]
        return false unless subs
        subs.any? { |prefix| topic.start_with?(prefix) }
      end


      # Called when a subscription command is received from a peer.
      # Override in subclasses to expose subscriptions to the
      # application (e.g. XPUB enqueues to recv_queue).
      #
      # @param conn [Protocol::ZMTP::Connection]
      # @param prefix [String]
      #
      def on_subscribe(conn, prefix)
        Routing.check_peer_subscriptions!(@subscriptions[conn], prefix, @engine.options.max_peer_subscriptions)

        @subscriptions[conn] << prefix.b.freeze
        @subscribe_all.add(conn) if prefix.empty?
        @subscriber_joined.resolve(conn) unless @subscriber_joined.resolved?
      end


      # Called when a cancel command is received from a peer.
      # Override in subclasses (e.g. XPUB enqueues to recv_queue).
      #
      # @param conn [Protocol::ZMTP::Connection]
      # @param prefix [String]
      #
      def on_cancel(conn, prefix)
        @subscriptions[conn]&.delete(prefix)
        @subscribe_all.delete(conn) if prefix.empty?
      end


      # Creates a per-connection send queue and starts its send pump.
      # Call from #connection_added.
      #
      # @param conn [Protocol::ZMTP::Connection]
      #
      def add_fan_out_send_connection(conn)
        q = Routing.build_queue(@engine.options.send_hwm, @engine.options.on_mute)
        @conn_queues[conn] = q
        @fan_out_queues    = @conn_queues.values.freeze
        start_conn_send_pump(conn, q)
      end


      # Removes the per-connection send queue. The pump itself is torn
      # down by the per-connection lifecycle barrier.
      # Call from #connection_removed.
      #
      # @param conn [Protocol::ZMTP::Connection]
      #
      def remove_fan_out_send_connection(conn)
        @subscribe_all.delete(conn)
        @conn_queues.delete(conn)&.close
        @fan_out_queues = @conn_queues.values.freeze
      end


      # Fans a message out to every connected peer's send queue.
      # Subscription filtering happens in the per-connection send pump so
      # that late-arriving subscriptions (e.g. inproc connect-before-subscribe)
      # are respected: a message enqueued before the async subscription listener
      # has processed SUBSCRIBE commands will still be delivered correctly.
      #
      # Per-connection queues honor the socket's on_mute strategy.
      # PUB/XPUB/RADIO default to :drop_newest so one slow subscriber
      # silently drops its own messages without stalling the publisher
      # or other subscribers. Applications can opt in to :block for
      # strict backpressure.
      #
      # Iterates a frozen snapshot: a :block enqueue can yield, and peers
      # that join or leave meanwhile must not break the iteration.
      #
      # @param parts [Array<String>]
      #
      def fan_out_enqueue(parts)
        @fan_out_queues.each do |q|
          Routing.enqueue_to_peer(q, parts)
        end
      end


      def start_subscription_listener(conn)
        @engine.spawn_conn_pump_task(conn, annotation: "subscription listener") do
          loop do
            frame = conn.read_frame

            case Protocol::ZMTP::Codec::Subscription.parse(frame)
            in [:subscribe, prefix]
              on_subscribe(conn, prefix)
            in [:cancel, prefix]
              on_cancel(conn, prefix)
            else
              next
            end
          end
        end
      end


      # Starts a dedicated send pump for one subscriber connection.
      # Uses write_wire (pre-encoded bytes) for non-encrypted TCP connections
      # to avoid re-encoding the same message N times during fan-out.
      # In conflate mode, drains the batch and keeps only the latest
      # message per topic before writing.
      #
      # @param conn [Protocol::ZMTP::Connection]
      # @param q [Async::LimitedQueue, DropQueue]
      #
      def start_conn_send_pump(conn, q)
        use_wire = conn.respond_to?(:write_wire) && !conn.encrypted?

        if @conflate
          start_conn_send_pump_conflate(conn, q)
        else
          start_conn_send_pump_normal(conn, q, use_wire)
        end
      end


      # Send pump variant for non-conflate fan-out: dequeues, batch-drains,
      # writes each subscribed message, then flushes once.
      #
      # @param conn [Protocol::ZMTP::Connection]
      # @param q [Async::LimitedQueue, DropQueue]
      # @param use_wire [Boolean] true iff the encoded wire bytes can
      #   be shared across peers (unencrypted ZMTP)
      # @return [Async::Task]
      #
      def start_conn_send_pump_normal(conn, q, use_wire)
        @engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          batch     = []
          aux_batch = []

          loop do
            Routing.dequeue_batch(q, batch) or break

            if write_matching_batch(conn, batch, use_wire, aux_batch)
              conn.flush
              batch.each { |parts| @engine.emit_verbose_msg_sent(conn, parts) }
            end
            batch.clear
          end
        end
      end


      # Writes every batch entry whose topic matches a subscription on
      # +conn+ to +conn+'s write buffer. Does not flush.
      #
      # @return [Boolean] true iff at least one message was written
      #
      def write_matching_batch(conn, batch, use_wire, aux_batch)
        if use_wire
          write_matching_batch_wire(conn, batch, aux_batch)
        else
          write_matching_batch_framed(conn, batch, aux_batch)
        end
      end


      def write_matching_batch_wire(conn, batch, wire_batch)
        batch.each do |parts|
          next unless subscribed?(conn, parts.first || EMPTY_BINARY)
          wire_batch << Protocol::ZMTP::Codec::Frame.encode_message(parts)
        end

        return false if wire_batch.empty?

        if wire_batch.size == 1
          conn.write_wire(wire_batch.first)
        else
          conn.write_wire_batch(wire_batch)
        end

        wire_batch.clear
        true
      end


      def write_matching_batch_framed(conn, batch, msg_batch)
        batch.each do |parts|
          next unless subscribed?(conn, parts.first || EMPTY_BINARY)
          msg_batch << parts
        end

        return false if msg_batch.empty?

        if msg_batch.size == 1
          conn.write_message(msg_batch.first)
        else
          conn.write_messages(msg_batch)
        end

        msg_batch.clear
        true
      end


      # Send pump variant for conflate mode: keeps only the latest
      # subscribed message per batch. Stale duplicates are dropped.
      #
      # @param conn [Protocol::ZMTP::Connection]
      # @param q [Async::LimitedQueue, DropQueue]
      # @return [Async::Task]
      #
      def start_conn_send_pump_conflate(conn, q)
        @engine.spawn_conn_pump_task(conn, annotation: "send pump") do
          batch = []

          loop do
            Routing.dequeue_batch(q, batch) or break

            # Keep only the latest message that matches the subscription.
            latest = batch.reverse.find do |parts|
              subscribed?(conn, parts.first || EMPTY_BINARY)
            end

            batch.clear
            next unless latest

            conn.write_message(latest)
            conn.flush
            @engine.emit_verbose_msg_sent(conn, latest)
          end
        end
      end

    end
  end
end
