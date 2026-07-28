# frozen_string_literal: true

module OMQ
  module Backend
    @engines = {}

    class << self
      def register(name, engine_class)
        key = name.to_sym

        if @engines.frozen?
          @engines = ::Ractor.make_shareable(@engines.merge(key => engine_class))
        else
          @engines[key] = engine_class
        end
      end


      def fetch(name)
        @engines[name.to_sym]
      end


      def registered?(name)
        @engines.key?(name.to_sym)
      end


      def freeze_for_ractors!
        return @engines if @engines.frozen?

        @engines = ::Ractor.make_shareable(@engines)
      end
    end
  end
end
