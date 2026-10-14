# frozen_string_literal: true

Warning[:experimental] = false

require "minitest/autorun"
require_relative "../../../test/async_spec"
require "async"
require "omq"
require "omq/transport/zstd_tcp"
