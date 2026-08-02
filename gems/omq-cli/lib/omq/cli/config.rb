# frozen_string_literal: true

module OMQ
  module CLI
    # Socket type names that only send messages.
    SEND_ONLY = %w[pub push scatter radio].freeze
    # Socket type names that only receive messages.
    RECV_ONLY = %w[sub pull gather dish].freeze


    # A bind or connect endpoint with its URL and direction.
    Endpoint = Data.define(:url, :bind?) do
      # @return [Boolean] true if this endpoint connects rather than binds
      def connect? = !bind?
    end


    # Frozen, Ractor-shareable configuration data class for a CLI invocation.
    Config = Data.define(
      :type_name,
      :endpoints,
      :connects,
      :binds,
      :in_endpoints,
      :out_endpoints,
      :data,
      :file,
      :format,
      :subscribes,
      :joins,
      :group,
      :identity,
      :target,
      :interval,
      :count,
      :delay,
      :timeout,
      :linger,
      :reconnect_ivl,
      :heartbeat_ivl,
      :send_hwm,
      :recv_hwm,
      :sndbuf,
      :rcvbuf,
      :conflate,
      :compress,
      :compress_level,
      :in_compress,
      :in_compress_level,
      :out_compress,
      :out_compress_level,
      :send_expr,
      :recv_expr,
      :parallel,
      :transient,
      :verbose,
      :timestamps,
      :quiet,
      :echo,
      :scripts,
      :recv_maxsz,
      :curve_server,
      :curve_server_key,
      :crypto,
      :backend,
      :ffi,
      :stdin_is_tty,
    ) do
      # @return [Boolean] true if this socket type only sends
      def send_only? = SEND_ONLY.include?(type_name)
      # @return [Boolean] true if this socket type only receives
      def recv_only? = RECV_ONLY.include?(type_name)
    end
  end
end
