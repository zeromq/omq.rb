# frozen_string_literal: true

module OMQ
  class QoS
    # Prepended onto {OMQ::Options} once omq-qos is loaded. Changes the
    # semantics of +Options#qos+ from "an Integer level" to "nil or an
    # {OMQ::QoS} instance":
    #
    #   options.qos        # => nil     (QoS 0, default)
    #   options.qos        # => <OMQ::QoS level=2 ...>
    #   options.qos_level  # => 0, 1, 2, or 3 (convenience Integer)
    #
    # The Integer form is still accepted by callers that never loaded
    # omq-qos — core omq keeps the Integer default. Once this extension
    # is installed the default resets to +nil+.
    #
    module OptionsExt
      def initialize(**kwargs)
        super
        @qos = nil
      end


      # Convenience: 0 / 1 / 2 / 3. nil-safe.
      #
      # @return [Integer]
      def qos_level
        @qos&.level || 0
      end
    end
  end
end


OMQ::Options.prepend(OMQ::QoS::OptionsExt)
