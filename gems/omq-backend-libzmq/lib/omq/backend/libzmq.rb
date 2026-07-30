# frozen_string_literal: true

require "omq"
require_relative "libzmq/native"
require_relative "libzmq/engine"

OMQ::Backend.register(:libzmq, OMQ::Backend::Libzmq::Engine)
