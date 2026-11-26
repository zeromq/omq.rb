# frozen_string_literal: true

module OMQ
  module Transport
    module ZstdTcp
      # Names what limited a sender during one adaptation window, as omq.rs
      # does (omq-proto `transform/adaptive.rs`). `Zrip::AdaptiveLevel` then
      # walks the level toward ratio (`:output`, `:input`) or speed (`:cpu`).
      #
      # One reactor thread compresses for every connection, and compression
      # below 64 KiB holds the GVL, so peers that are all busy all read as
      # `:cpu`. That is what they are.
      #
      module Bottleneck
        module_function


        # @param elapsed [Float] seconds the window covers
        # @param blocked [Float] seconds writes stalled
        # @param batches [Integer] batches written
        # @param backlogged [Integer] batches that left input queued
        # @param overflowed [Integer] backlogged batches that stopped at a
        #   batch limit
        # @param input_full [Integer] batches taken from a full send queue
        # @return [Symbol, nil] `:output`, `:cpu`, `:input`, or nil when no
        #   bottleneck is clear
        def classify(elapsed:, blocked:, batches:, backlogged:, overflowed:, input_full:)
          return nil if elapsed <= 0 || batches.zero?

          # Writes stalled for at least 1/16 of the window.
          return :output if blocked > 0 && blocked * 16 >= elapsed

          # The send queue filled, or more than 1/8 of the batches stopped
          # at their limit with input left. A shorter backlog only grows the
          # batches.
          if input_full > 0 || overflowed * 8 > batches
            return blocked * 64 < elapsed ? :cpu : nil
          end

          backlogged.zero? ? :input : nil
        end
      end
    end
  end
end
