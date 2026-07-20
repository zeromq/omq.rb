# frozen_string_literal: true

module OMQ
  module Backend
    @engines = {}

    class << self
      def register(name, engine_class)
        @engines[name.to_sym] = engine_class
      end


      def fetch(name)
        @engines[name.to_sym]
      end


      def registered?(name)
        @engines.key?(name.to_sym)
      end
    end
  end
end
