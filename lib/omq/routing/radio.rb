# frozen_string_literal: true

module OMQ
  module Routing
    # RADIO socket routing: group-based fan-out to DISH peers.
    #
    # Like PUB/FanOut but with exact group matching and JOIN/LEAVE
    # commands instead of SUBSCRIBE/CANCEL.
    #
    # Messages are sent as two frames on the wire:
    #   group (MORE=1) + body (MORE=0)
    #
    class Radio
      # Sentinel used for UDP connections that have no group filter:
      # any group is considered a match.
      #
      ANY_GROUPS = Object.new.tap { |o| o.define_singleton_method(:include?) { |_| true } }.freeze


      # @return [Async::LimitedQueue]
      #
      attr_reader :send_queue
      attr_reader :subscriber_joined


      # @param engine [Engine]
      #
      def initialize(engine)
        @engine            = engine
        @connections       = []
        @groups            = {} # connection => Set of joined groups (or ANY_GROUPS for UDP)
        @send_queue        = Routing.build_queue(engine.options.send_hwm, :block)
        @subscriber_joined = Async::Promise.new
        @on_mute           = engine.options.on_mute
        @send_pump_started = false
        @conflate          = engine.options.conflate
        @written           = Set.new
        @latest            = {} if @conflate
      end


      # RADIO is write-only.
      #
      def recv_queue
        raise "RADIO sockets cannot receive"
      end


      # No-op; RADIO has no recv queue to unblock.
      #
      def unblock_recv
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_added(connection)
        @connections << connection
        if connection.respond_to?(:read_frame)
          @groups[connection] = Set.new
          start_group_listener(connection)
        else
          @groups[connection] = ANY_GROUPS  # UDP: fan-out to all groups
        end
        start_send_pump unless @send_pump_started
      end


      # @param connection [Protocol::ZMTP::Connection]
      #
      def connection_removed(connection)
        @connections.delete(connection)
        @groups.delete(connection)
      end


      # Enqueues a message for sending.
      #
      # @param parts [Array<String>] [group, body]
      #
      def enqueue(parts)
        @send_queue.enqueue(parts)
      end


      # True when the send queue is empty.
      #
      def send_queues_drained?
        @send_queue.empty?
      end


      private


      def muted?(conn)
        return false if @on_mute == :block
        q = conn.direct_recv_queue if conn.respond_to?(:direct_recv_queue)
        q&.respond_to?(:limited?) && q.limited?
      end


      def start_send_pump
        @send_pump_started = true
        @engine.spawn_pump_task(annotation: "send pump", parent: @engine.barrier) do
          batch = []

          loop do
            @send_pump_idle = true
            Routing.dequeue_batch(@send_queue, batch)
            @send_pump_idle = false

            @written.clear

            if @conflate
              # Keep only the last matching message per connection.
              @latest.clear
              batch.each do |parts|
                group = parts[0]
                body  = parts[1] || EMPTY_BINARY
                @connections.each do |conn|
                  next unless @groups[conn]&.include?(group)
                  @latest[conn] = [group, body]
                end
              end
              @latest.each do |conn, msg|
                next if muted?(conn)
                begin
                  conn.write_message(msg)
                  @written << conn
                rescue *CONNECTION_LOST
                end
              end
            else
              batch.each do |parts|
                group      = parts[0]
                body       = parts[1] || EMPTY_BINARY
                msg        = [group, body]
                wire_bytes = nil

                @connections.each do |conn|
                  next unless @groups[conn]&.include?(group)
                  next if muted?(conn)
                  begin
                    if conn.respond_to?(:curve?) && conn.curve?
                      conn.write_message(msg)
                    elsif conn.respond_to?(:write_wire)
                      wire_bytes ||= Protocol::ZMTP::Codec::Frame.encode_message(msg)
                      conn.write_wire(wire_bytes)
                    else
                      conn.write_message(msg)
                    end
                    @written << conn
                  rescue *CONNECTION_LOST
                  end
                end
              end
            end

            @written.each do |conn|
              conn.flush
            rescue *CONNECTION_LOST
            end

            batch.clear
          end
        end
      end


      def start_group_listener(conn)
        @engine.spawn_conn_pump_task(conn, annotation: "group listener") do
          loop do
            frame = conn.read_frame
            next unless frame.command?
            cmd = Protocol::ZMTP::Codec::Command.from_body(frame.body)
            case cmd.name
            when "JOIN"
              @groups[conn]&.add(cmd.data)
              @subscriber_joined.resolve(conn) unless @subscriber_joined.resolved?
            when "LEAVE"
              @groups[conn]&.delete(cmd.data)
            end
          end
        end
      end

    end
  end
end
