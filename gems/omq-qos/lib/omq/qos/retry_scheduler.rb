# frozen_string_literal: true

module OMQ
  class QoS
    # Computes exponential backoff delays for QoS 3 retries.
    #
    # +range.begin+ is the first-retry delay. Each subsequent retry
    # doubles the delay, capped at +range.end+.
    #
    module RetryScheduler
      # @param retry_count [Integer] number of retries already attempted
      #   (0 before the first retry)
      # @param range [Range] +retry_backoff+ configuration (min..max)
      # @return [Numeric] seconds to sleep before the next retry attempt
      def self.delay(retry_count, range)
        return range.begin if retry_count <= 0
        scaled = range.begin * (2 ** retry_count)
        scaled < range.end ? scaled : range.end
      end
    end
  end
end
