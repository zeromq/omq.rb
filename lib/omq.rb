# frozen_string_literal: true

require "protocol/zmtp"
require "io/stream"


# Core
require_relative "omq/version"
require_relative "omq/constants"
require_relative "omq/backend"
require_relative "omq/reactor"
require_relative "omq/options"
require_relative "omq/routing"
require_relative "omq/routing/round_robin"
require_relative "omq/routing/fan_out"
require_relative "omq/routing/pair"
require_relative "omq/routing/req"
require_relative "omq/routing/rep"
require_relative "omq/routing/dealer"
require_relative "omq/routing/router"
require_relative "omq/routing/pub"
require_relative "omq/routing/sub"
require_relative "omq/routing/xpub"
require_relative "omq/routing/xsub"
require_relative "omq/routing/push"
require_relative "omq/routing/pull"
require_relative "omq/engine"
OMQ::Backend.register(:ruby, OMQ::Engine)

# Transport
require_relative "omq/transport/inproc"
require_relative "omq/transport/tcp"
require_relative "omq/transport/ipc"

# Mixins
require_relative "omq/queue_interface"
require_relative "omq/readable"
require_relative "omq/writable"
require_relative "omq/single_frame"

# Socket types
require_relative "omq/socket"
require_relative "omq/req_rep"
require_relative "omq/router_dealer"
require_relative "omq/pub_sub"
require_relative "omq/push_pull"
require_relative "omq/pair"

# For the purists.
ØMQ = OMQ
