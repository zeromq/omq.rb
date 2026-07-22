# frozen_string_literal: true

require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("omq/rust/omq_backend_rust") do |r|
  r.profile = ENV.fetch("RB_SYS_CARGO_PROFILE", :release).to_sym
end
