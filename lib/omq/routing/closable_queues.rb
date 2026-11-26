# frozen_string_literal: true

module OMQ
  module Routing
    # Closes the queues a routing strategy owns once its socket is closed.
    #
    # Fibers blocked in a send raise +Async::Queue::ClosedError+ (or
    # +ClosedQueueError+ for a {DropQueue}), which {Engine#enqueue_send}
    # turns into {SocketClosedError}. Fibers blocked in a receive get
    # +nil+, like +Thread::Queue#pop+ on a closed queue.
    #
    # Relies on the ivar names the built-in strategies share:
    # +@recv_queue+, +@send_queue+, +@staging_queue+ and the
    # per-connection +@conn_queues+ Hash.
    #
    module ClosableQueues
      # @return [void]
      #
      def close_queues
        @recv_queue&.close
        @send_queue&.close
        @staging_queue&.close
        @conn_queues&.each_value(&:close)
      end

    end
  end
end
