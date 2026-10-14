# frozen_string_literal: true

module OMQ
  module Transport
    module Inproc
      # A direct in-process pipe that transfers Ruby arrays through queues.
      #
      # Implements the same interface as Connection so routing strategies
      # can use it transparently.
      #
      # When a routing strategy sets {#direct_recv_queue} on a pipe,
      # {#send_message} enqueues directly into the peer's recv queue,
      # bypassing the intermediate pipe queues and the recv pump task.
      # This reduces inproc from 3 queue hops to 2 (send_queue →
      # recv_queue), eliminating the internal pipe queue in between.
      #
      class Pipe
        # @return [String] peer's socket type
        #
        attr_reader :peer_socket_type


        # @return [Integer] always 3 — inproc peers are OMQ
        #
        def peer_major
          3
        end


        # @return [Integer] always 1 — inproc peers are OMQ (ZMTP 3.1)
        #
        def peer_minor
          1
        end


        # @return [String] peer's identity
        #
        attr_reader :peer_identity


        # @return [Pipe, nil] the other end of this pipe pair
        #
        attr_accessor :peer


        # @return [Async::LimitedQueue, nil] when set, {#send_message}
        #   enqueues directly here instead of using the internal queue
        #
        attr_reader :direct_recv_queue


        # @param send_queue [Async::Queue, nil] outgoing command queue
        #   (nil for non-PUB/SUB types that don't exchange commands)
        # @param receive_queue [Async::Queue, nil] incoming command queue
        # @param peer_identity [String]
        # @param peer_type [String]
        #
        def initialize(send_queue: nil, receive_queue: nil, peer_identity:, peer_type:)
          @send_queue            = send_queue
          @receive_queue         = receive_queue
          @peer_identity         = peer_identity || "".b
          @peer_socket_type      = peer_type
          @closed                = false
          @peer                  = nil
          @direct_recv_queue     = nil
          @direct_recv_transform = nil
          @pending_direct        = nil
        end


        # Wires up the direct recv fast-path. Called once by the recv
        # pump when the receiving side of an inproc pipe pair is set up.
        # After this, peer-side {#send_message} calls enqueue straight
        # into +queue+ instead of hopping through the intermediate pipe
        # queue and a recv pump fiber.
        #
        # Drains any messages the peer buffered into +@pending_direct+
        # before the queue was available.
        #
        # @param queue [Async::LimitedQueue]
        # @param transform [Proc, nil] optional per-message transform
        # @return [void]
        #
        def wire_direct_recv(queue, transform)
          @direct_recv_transform = transform
          @direct_recv_queue     = queue

          return unless @pending_direct

          @pending_direct.each { |msg| deliver_direct(msg) }
          @pending_direct = nil
        end


        # Sends a multi-frame message.
        #
        # @param parts [Array<String>]
        # @return [void]
        #
        def send_message(parts)
          raise IOError, "closed" if @closed

          # Writable#send guarantees frozen parts, but a frozen non-BINARY
          # part (e.g. a `# frozen_string_literal: true` literal) can't be
          # re-tagged in place. Inproc receivers see the parts directly, so
          # upgrade that one case to fresh BINARY copies to keep the
          # receive contract uniform with TCP/IPC.
          #
          # Non-String parts pass through untouched — plugins like
          # omq-ractor's ShareableConnection carry arbitrary Ruby objects
          # over inproc.
          if parts.any? { |p| p.is_a?(String) && p.encoding != Encoding::BINARY }
            parts = parts.map { |p| !p.is_a?(String) || p.encoding == Encoding::BINARY ? p : p.b.freeze }.freeze
          end

          if @direct_recv_queue
            deliver_direct(parts)
          elsif @send_queue
            @send_queue.enqueue(parts)
          else
            (@pending_direct ||= []) << parts
          end
        end


        alias write_message send_message


        # Batched form, for parity with Protocol::ZMTP::Connection. The
        # work-stealing pumps call this when they dequeue more than one
        # message at once; Pipe just loops — no mutex to amortize.
        #
        # @param messages [Array<Array<String>>]
        # @return [void]
        #
        def write_messages(messages)
          messages.each { |parts| send_message(parts) }
        end


        # @return [Boolean] always false; inproc pipes are never encrypted
        #
        def encrypted?
          false
        end


        # No-op — inproc has no IO buffer to flush.
        #
        # @return [nil]
        #
        def flush
          nil
        end


        # Receives a multi-frame message.
        #
        # @return [Array<String>]
        # @raise [EOFError] if closed
        #
        def receive_message
          loop do
            item = @receive_queue.dequeue or raise EOFError, "connection closed"

            if item.is_a?(Array) && item.first == :command
              if block_given?
                yield Protocol::ZMTP::Codec::Frame.new(item[1].to_body, command: true)
              end

              next
            end

            return item
          end
        end


        # Sends a command via the internal command queue.
        # Only available for PUB/SUB-family pipes.
        #
        # @param command [Protocol::ZMTP::Codec::Command]
        #
        def send_command(command)
          raise IOError, "closed" if @closed
          @send_queue.enqueue([:command, command])
        end


        # Reads one frame. Used by PUB/XPUB subscription listeners,
        # which must see both the legacy message-form subscription
        # (ZMTP 3.0) and the command-form (ZMTP 3.1).
        #
        # @return [Protocol::ZMTP::Codec::Frame]
        #
        def read_frame
          item = @receive_queue.dequeue or raise EOFError, "connection closed"

          if item.is_a?(Array) && item.first == :command
            Protocol::ZMTP::Codec::Frame.new(item[1].to_body, command: true)
          else
            Protocol::ZMTP::Codec::Frame.new(item.first || "".b)
          end
        end


        # Closes this pipe end and sends a nil sentinel to the peer.
        #
        # @return [void]
        #
        def close
          return if @closed
          @closed = true
          @send_queue&.enqueue(nil) # close sentinel
        end


        private


        def deliver_direct(parts)
          transformed = apply_transform(parts)
          @direct_recv_queue.enqueue(transformed) if transformed
        end


        def apply_transform(parts)
          if @direct_recv_transform
            @direct_recv_transform.call(parts)
          else
            parts
          end
        end

      end
    end
  end
end
