# frozen_string_literal: true

module OMQ
  module Transport
    module WebSocket
      # Prepended onto OMQ::Options at require time.
      # Adds the per-socket knobs needed by ws:// and wss://.
      module OptionsExt

        DEFAULT_SUBPROTOCOLS = %w[ZWS2.0/NULL ZWS2.0].freeze
        DEFAULT_PATH         = "/"


        attr_accessor :tls_context
        attr_accessor :ws_subprotocols
        attr_accessor :ws_path


        def initialize(*, **)
          super
          @tls_context     = nil
          @ws_subprotocols = DEFAULT_SUBPROTOCOLS
          @ws_path         = DEFAULT_PATH
        end

      end
    end
  end
end
