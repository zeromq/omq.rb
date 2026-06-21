#!/usr/bin/env ruby
# frozen_string_literal: true

# Generate PUB/SUB backend comparison chart from JSONL results.
#
# Reads:  ~/.cache/omq/ruby/results_pubsub.jsonl
# Writes: doc/charts/pubsub/backends_tcp.svg
#
# Usage: ruby bench/gen_pubsub_chart.rb

require "json"
require "fileutils"
require_relative "chart_helper"

CACHE_DIR  = File.join(ENV.fetch("XDG_CACHE_HOME", File.join(Dir.home, ".cache")), "omq", "ruby")
JSONL_PATH = File.join(CACHE_DIR, "results_pubsub.jsonl")
REPO_ROOT  = File.expand_path("..", __dir__)
OUTPUT     = File.join(REPO_ROOT, "doc", "charts", "pubsub", "backends_tcp.svg")

COLORS = {
  "ruby" => "#eab308",
  "rust" => "#dc2626",
  "ffi"  => "#7c3aed",
}

LABELS = {
  "ruby" => "pure Ruby + YJIT",
  "rust" => "Rust (omq-tokio)",
  "ffi"  => "FFI (libzmq)",
}

SERIES_ORDER = ["ruby", "rust", "ffi"]


def load_data
  unless File.exist?(JSONL_PATH)
    $stderr.puts "ERROR: #{JSONL_PATH} not found. Run bench_pubsub.rb first."
    exit 1
  end

  rows = File.readlines(JSONL_PATH).filter_map do |line|
    line = line.strip
    next if line.empty?

    row = JSON.parse(line, symbolize_names: true)
    row if row[:pattern] == "pubsub"
  end

  if rows.empty?
    $stderr.puts "ERROR: no pubsub rows in #{JSONL_PATH}"
    exit 1
  end

  newest = {}
  rows.each do |r|
    key = [r[:backend], r[:msg_size]]
    if !newest[key] || r[:run_id] > newest[key][:run_id]
      newest[key] = r
    end
  end

  sizes_set = Set.new
  series    = {}

  newest.each_value do |r|
    backend = r[:backend]
    sz      = r[:msg_size]
    sizes_set << sz

    elapsed  = r[:elapsed] || 0
    cpu_time = r[:cpu_time] || 0
    cpu_pct  = elapsed > 0 ? cpu_time / elapsed * 100.0 : 0

    series[backend] ||= {}
    series[backend][sz] = {
      msgs_s:  r[:msgs_s],
      mbps:    r[:mbps],
      cpu_pct: cpu_pct,
    }
  end

  [sizes_set.sort, series]
end


def main
  sizes, series = load_data
  hw = ChartHelper.detect_hardware

  present = SERIES_ORDER.select { |k| series.key?(k) }
  backends_label = present.map { |k| LABELS[k] }.join(" vs ")

  svg = ChartHelper.generate_single_panel_svg(
    title:        "PUB/SUB throughput: TCP loopback, 2-process (#{backends_label})",
    hw_label:     hw,
    sizes:        sizes,
    series:       series,
    colors:       COLORS,
    labels:       LABELS,
    series_order: SERIES_ORDER,
  )

  FileUtils.mkdir_p(File.dirname(OUTPUT))
  File.write(OUTPUT, svg)
  $stderr.puts "Written: #{OUTPUT}"
end


main
