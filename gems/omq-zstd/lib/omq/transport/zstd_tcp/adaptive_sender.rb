# frozen_string_literal: true

require "async/clock"

require_relative "bottleneck"

module OMQ
  module Transport
    module ZstdTcp
      # One connection's adaptive compression level: its own frame codec and
      # `Zrip::AdaptiveLevel`, and the counts of the current window. The
      # socket's {Codec} keeps the dictionary and its training.
      #
      class AdaptiveSender
        # A window closes after at least this long...
        WINDOW_SECONDS = 0.1

        # ...and this much input, as in omq.rs (zrip's `MIN_WINDOW_BYTES`).
        WINDOW_BYTES = 256 * 1024

        # A write that takes longer than this waited on the link; the time
        # beyond it counts as stalled. Copying a batch into the kernel takes
        # well under it, and counting that made CPU-bound windows unclear.
        WRITE_GRACE = 0.001

        # The send pump's batch limits (`Routing::RoundRobin`). A batch that
        # reaches one with input still queued overflowed.
        BATCH_MSG_CAP  = Routing::RoundRobin::BATCH_MSG_CAP
        BATCH_BYTE_CAP = Routing::RoundRobin::BATCH_BYTE_CAP


        # @return [Codec] the socket's codec
        attr_reader :codec

        # @return [Float] when the current window started
        attr_reader :window_started


        # @param codec [Codec] the socket's codec, with the level range
        # @param send_hwm [Integer, nil] the socket's send HWM. A batch this
        #   large was taken from a full queue.
        # @param queued [#call, nil] whether the socket has messages queued
        #   to send, asked as each batch is written. Nil counts no batch as
        #   backlogged.
        def initialize(codec, send_hwm:, queued:)
          @codec       = codec
          @send_hwm    = send_hwm if send_hwm&.positive?
          @queued      = queued
          @adaptive    = Zrip::AdaptiveLevel.new(codec.level_range, start: START_LEVEL)
          @dict        = codec.send_dict_bytes
          @frame_codec = codec.build_frame_codec(@adaptive.level)
          start_window(Async::Clock.now)
        end


        # The codec to compress with. It follows the socket's dictionary once
        # training installs one, at the current level.
        #
        # @return [Zrip::FrameCodec]
        def frame_codec
          unless @codec.send_dict_bytes.equal?(@dict)
            record(@frame_codec.take_stats)
            @dict        = @codec.send_dict_bytes
            @frame_codec = @codec.build_frame_codec(@frame_codec.level)
          end

          @frame_codec
        end


        # Counts one written batch of messages.
        #
        # @param messages [Array<Array<String>>]
        # @return [void]
        def note_batch(messages)
          bytes = 0
          messages.each { |parts| parts.each { |part| bytes += part.bytesize } }

          # A batch that reaches a limit can still be all there was: one
          # large message reaches the byte limit alone.
          capped = messages.size >= BATCH_MSG_CAP || bytes >= BATCH_BYTE_CAP
          left   = @queued&.call

          @batches    += 1
          @backlogged += 1 if left
          @overflowed += 1 if left && capped
          @input_full += 1 if @send_hwm && messages.size >= @send_hwm
        end


        # @param seconds [Float] time a write or flush took
        # @return [void]
        def note_write(seconds)
          @blocked += seconds - WRITE_GRACE if seconds > WRITE_GRACE
        end


        # Closes the window if it is long and large enough, and moves the
        # level.
        #
        # @param now [Float]
        # @return [void]
        def maybe_adapt(now = Async::Clock.now)
          elapsed = now - @window_started
          return if elapsed < WINDOW_SECONDS

          record(@frame_codec.take_stats)
          return if @plain < WINDOW_BYTES

          bottleneck = Bottleneck.classify(
            elapsed:    elapsed,
            blocked:    @blocked,
            batches:    @batches,
            backlogged: @backlogged,
            overflowed: @overflowed,
            input_full: @input_full,
          )
          level = @adaptive.update(plain: @plain, compressed: @compressed, busy: @busy, bottleneck: bottleneck)
          @frame_codec.level = level unless level == @frame_codec.level

          start_window(now)
        end


        private


        def record(stats)
          @plain      += stats[:plain]
          @compressed += stats[:compressed]
          @busy       += stats[:busy]
        end


        def start_window(now)
          @window_started = now
          @plain          = 0
          @compressed     = 0
          @busy           = 0.0
          @blocked        = 0.0
          @batches        = 0
          @backlogged     = 0
          @overflowed     = 0
          @input_full     = 0
        end

      end
    end
  end
end
