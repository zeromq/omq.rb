# frozen_string_literal: true

require "ffi"

module OMQ
  module FFI
    # Minimal libzmq FFI bindings — only what OMQ needs.
    #
    module Libzmq
      extend ::FFI::Library
      ffi_lib ["libzmq.so.5", "libzmq.5.dylib", "libzmq"]

      # Context
      attach_function :zmq_ctx_new,  [], :pointer
      attach_function :zmq_ctx_term,     [:pointer], :int
      attach_function :zmq_ctx_shutdown, [:pointer], :int

      # Socket
      attach_function :zmq_socket,     [:pointer, :int], :pointer
      attach_function :zmq_close,      [:pointer], :int
      attach_function :zmq_bind,       [:pointer, :string], :int
      attach_function :zmq_connect,    [:pointer, :string], :int
      attach_function :zmq_disconnect, [:pointer, :string], :int
      attach_function :zmq_unbind,     [:pointer, :string], :int

      # Message
      attach_function :zmq_msg_init,      [:pointer], :int
      attach_function :zmq_msg_init_size, [:pointer, :size_t], :int
      attach_function :zmq_msg_data,      [:pointer], :pointer
      attach_function :zmq_msg_size,      [:pointer], :size_t
      attach_function :zmq_msg_close,     [:pointer], :int
      attach_function :zmq_msg_send,      [:pointer, :pointer, :int], :int
      attach_function :zmq_msg_recv,      [:pointer, :pointer, :int], :int

      # Socket options
      attach_function :zmq_setsockopt, [:pointer, :int, :pointer, :size_t], :int
      attach_function :zmq_getsockopt, [:pointer, :int, :pointer, :pointer], :int

      # Group membership (RADIO/DISH) — draft API, may not be available
      begin
        attach_function :zmq_join,  [:pointer, :string], :int
        attach_function :zmq_leave, [:pointer, :string], :int
      rescue ::FFI::NotFoundError
        # libzmq built without ZMQ_BUILD_DRAFT_API
      end


      # Error
      attach_function :zmq_errno,    [], :int
      attach_function :zmq_strerror, [:int], :string

      # Socket types
      ZMQ_PAIR    = 0
      ZMQ_PUB     = 1
      ZMQ_SUB     = 2
      ZMQ_REQ     = 3
      ZMQ_REP     = 4
      ZMQ_DEALER  = 5
      ZMQ_ROUTER  = 6
      ZMQ_PULL    = 7
      ZMQ_PUSH    = 8
      ZMQ_XPUB    = 9
      ZMQ_XSUB    = 10
      ZMQ_SERVER  = 12
      ZMQ_CLIENT  = 13
      ZMQ_RADIO   = 14
      ZMQ_DISH    = 15
      ZMQ_GATHER  = 16
      ZMQ_SCATTER = 17
      ZMQ_PEER    = 19
      ZMQ_CHANNEL = 20


      # Socket type name → constant
      SOCKET_TYPES = {
        PAIR: ZMQ_PAIR, PUB: ZMQ_PUB, SUB: ZMQ_SUB,
        REQ: ZMQ_REQ, REP: ZMQ_REP,
        DEALER: ZMQ_DEALER, ROUTER: ZMQ_ROUTER,
        PULL: ZMQ_PULL, PUSH: ZMQ_PUSH,
        XPUB: ZMQ_XPUB, XSUB: ZMQ_XSUB,
        SERVER: ZMQ_SERVER, CLIENT: ZMQ_CLIENT,
        RADIO: ZMQ_RADIO, DISH: ZMQ_DISH,
        GATHER: ZMQ_GATHER, SCATTER: ZMQ_SCATTER,
        PEER: ZMQ_PEER, CHANNEL: ZMQ_CHANNEL,
      }.freeze

      # Send/recv flags
      ZMQ_DONTWAIT = 1
      ZMQ_SNDMORE  = 2


      # Socket options
      ZMQ_IDENTITY     = 5
      ZMQ_SUBSCRIBE    = 6
      ZMQ_UNSUBSCRIBE  = 7
      ZMQ_RCVMORE      = 13
      ZMQ_FD           = 14
      ZMQ_EVENTS       = 15
      ZMQ_LINGER       = 17
      ZMQ_SNDHWM       = 23
      ZMQ_RCVHWM       = 24
      ZMQ_RCVTIMEO     = 27
      ZMQ_SNDTIMEO     = 28
      ZMQ_MAXMSGSIZE   = 22
      ZMQ_LAST_ENDPOINT = 32
      ZMQ_ROUTER_MANDATORY = 33
      ZMQ_RECONNECT_IVL     = 18
      ZMQ_RECONNECT_IVL_MAX = 21
      ZMQ_CONFLATE     = 54


      # zmq_msg_t is 64 bytes on all platforms
      MSG_T_SIZE = 64


      # Allocates a zmq_msg_t on the heap.
      #
      # @return [FFI::MemoryPointer]
      #
      def self.alloc_msg
        ::FFI::MemoryPointer.new(MSG_T_SIZE)
      end


      # Raises an error with the current zmq_errno message.
      #
      def self.check!(rc, label = "zmq")
        return rc if rc >= 0
        errno = zmq_errno
        raise "#{label}: #{zmq_strerror(errno)} (errno #{errno})"
      end
    end
  end
end
