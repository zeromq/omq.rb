# frozen_string_literal: true

module OMQ
  class Engine
    # Endpoint parsing and transport lookup.
    #
    module Endpoints
      # Returns the transport object (Dialer or Listener) for an endpoint.
      # Used by {ConnectionLifecycle#ready!} to call +#wrap_connection+.
      #
      # @param endpoint [String]
      # @return [Dialer, Listener, nil]
      #
      def transport_object_for(endpoint)
        @dialers[endpoint] || @listeners[endpoint]
      end


      # Looks up the transport module for an endpoint URI.
      #
      # @param endpoint [String] endpoint URI (e.g. "tcp://...", "ruby://...")
      # @return [Module] the transport module
      # @raise [ArgumentError] if the scheme is not registered
      #
      def transport_for(endpoint)
        scheme    = scheme_for(endpoint)
        transport = registered_transport(scheme)

        unless transport
          raise ArgumentError, "unsupported transport: #{endpoint}"
        end

        transport
      end


      private


      # Delegates endpoint validation to the transport if it defines one.
      # Called from {#connect} so a bad URI fails fast instead of landing
      # in a reconnect loop.
      #
      # @param endpoint [String]
      # @raise [ArgumentError] if the transport rejects the endpoint
      #
      def validate_endpoint!(endpoint, transport)
        if transport.respond_to?(:validate_endpoint!)
          transport.validate_endpoint!(endpoint)
        end
      end


      # Resolves transport aliases before the endpoint is stored in
      # listener, dialer, or connection maps.
      #
      # @param endpoint [String]
      # @return [Array(String, Module)]
      #
      def resolve_endpoint(endpoint)
        transport = transport_for(endpoint)
        if transport.respond_to?(:canonical_endpoint)
          endpoint = transport.canonical_endpoint(endpoint)
        end

        [endpoint, transport]
      end


      # Best-effort normalization for disconnect/unbind. Unknown schemes
      # stay as-is so those calls remain no-ops instead of raising.
      #
      # @param endpoint [String]
      # @return [String]
      #
      def canonical_endpoint(endpoint)
        transport = registered_transport(scheme_for(endpoint))
        return endpoint unless transport&.respond_to?(:canonical_endpoint)

        transport.canonical_endpoint(endpoint)
      end


      # Looks up a registered transport. If no real inproc transport is
      # registered, inproc:// falls back to the Ruby in-process transport.
      #
      # @param scheme [String, nil]
      # @return [Module, nil]
      #
      def registered_transport(scheme)
        self.class.transports[scheme] ||
          (scheme == "inproc" ? self.class.transports["ruby"] : nil)
      end


      # Extracts the URI scheme from an endpoint.
      #
      # @param endpoint [String]
      # @return [String, nil]
      #
      def scheme_for(endpoint)
        endpoint[/\A([^:]+):\/\//, 1]
      end

    end
  end
end
