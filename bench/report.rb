# frozen_string_literal: true

# Benchmark regression report.
#
# Reads bench/results.jsonl and compares the last N runs,
# highlighting regressions and improvements.
#
# Usage:
#   ruby bench/report.rb                  # latest vs previous
#   ruby bench/report.rb --all            # show all measurements
#   ruby bench/report.rb --runs 5         # compare latest vs oldest-of-5
#   ruby bench/report.rb --threshold 10   # 10% noise band
#   ruby bench/report.rb --pattern push_pull

require 'json'
require 'optparse'

RESULTS_PATH = File.join(__dir__, "results.jsonl")

options = { runs: 2, threshold: 5, all: false, pattern: nil, update_readme: false }

OptionParser.new do |o|
  o.banner = "Usage: ruby bench/report.rb [options]"
  o.on("--runs N", Integer, "Number of runs to compare (default 2)")     { |v| options[:runs] = v }
  o.on("--threshold PCT", Float, "Noise band percentage (default 5)")    { |v| options[:threshold] = v }
  o.on("--all", "Show all measurements, not just outliers")              { options[:all] = true }
  o.on("--pattern NAME", "Filter to a specific pattern (e.g. push_pull)") { |v| options[:pattern] = v }
  o.on("--update-readme", "Regenerate bench/README.md tables from latest run") { options[:update_readme] = true }
end.parse!

unless File.exist?(RESULTS_PATH)
  abort "No results file at #{RESULTS_PATH}. Run benchmarks first."
end

rows = File.readlines(RESULTS_PATH).map { |line| JSON.parse(line, symbolize_names: true) }

# ---- --update-readme: regenerate bench/README.md tables from latest run ----

if options[:update_readme]
  README_PATH = File.join(__dir__, "README.md")
  TRANSPORTS  = %w[inproc ipc tcp].freeze
  SIZE_LABELS = {
    8       => "8 B",
    32      => "32 B",
    128     => "128 B",
    512     => "512 B",
    2048    => "2 KiB",
    8192    => "8 KiB",
    32_768  => "32 KiB",
    131_072 => "128 KiB",
    524_288 => "512 KiB",
  }.freeze

  abort "No runs found in #{RESULTS_PATH}" if rows.empty?

  # Look up the most recent row for a cell across all history. Falling back
  # to older runs means a partial bench (e.g. only push_pull, 1 peer, 128 B)
  # refreshes just the cells it covers and leaves untouched cells showing
  # their last known good value, instead of clobbering them with "—".
  cell = lambda do |pattern, transport, peers, msg_size|
    rows.reverse_each.find do |x|
      x[:pattern] == pattern && x[:transport] == transport
      && x[:peers] == peers && x[:msg_size] == msg_size
    end
  end

  fmt_rate = lambda do |v|
    next nil unless v
    if    v >= 1e6 then "%.2fM msg/s" % (v / 1e6)
    elsif v >= 1e3 then "%.1fk msg/s" % (v / 1e3)
    else                "%.0f msg/s"  % v
    end
  end

  # Trailing * flags a nominal figure: payloads are shared (not `.dup`'d),
  # so the sender and receiver operate on the same String object. For
  # inproc that means no memory copy happens at all — the product
  # (msg/s × size) overstates actual memory bandwidth. For IPC/TCP the
  # kernel still copies the bytes, but we apply the same asterisk for
  # consistency since the benchmark never attributes bytes to per-message
  # allocator work.
  fmt_mbps = lambda do |v|
    next nil unless v
    if    v >= 1000 then "%.2f GB/s*" % (v / 1000.0)
    elsif v >= 100  then "%.0f MB/s*" % v
    elsif v >= 10   then "%.1f MB/s*" % v
    else                 "%.2f MB/s*" % v
    end
  end

  fmt_throughput = lambda do |r|
    next "—" unless r
    rate = fmt_rate.call(r[:msgs_s])
    mb   = fmt_mbps.call(r[:mbps])
    [rate, mb].compact.join(" / ")
  end

  fmt_latency_us = lambda do |r|
    v = r && r[:msgs_s]
    next "—" unless v && v > 0
    us = 1_000_000.0 / v
    if    us >= 100 then "%.0f µs"  % us
    elsif us >= 10  then "%.1f µs"  % us
    else                 "%.2f µs"  % us
    end
  end

  build_push_pull = lambda do
    sizes = SIZE_LABELS.keys
    out   = +"\n"
    [1, 3].each do |peers|
      have_any = sizes.any? { |s| TRANSPORTS.any? { |t| cell.call("push_pull", t, peers, s) } }
      next unless have_any
      out << "### #{peers} peer#{'s' if peers > 1}\n\n"
      out << "| Message size | #{TRANSPORTS.join(' | ')} |\n"
      out << "|---|#{TRANSPORTS.map { '---' }.join('|')}|\n"
      sizes.each do |size|
        values = TRANSPORTS.map { |t| fmt_throughput.call(cell.call("push_pull", t, peers, size)) }
        out << "| #{SIZE_LABELS[size]} | #{values.join(' | ')} |\n"
      end
      out << "\n"
    end
    out
  end

  build_req_rep = lambda do
    sizes = SIZE_LABELS.keys
    out   = +"\n| Message size | #{TRANSPORTS.join(' | ')} |\n"
    out << "|---|#{TRANSPORTS.map { '---' }.join('|')}|\n"
    sizes.each do |size|
      values = TRANSPORTS.map { |t| fmt_latency_us.call(cell.call("req_rep", t, 1, size)) }
      out << "| #{SIZE_LABELS[size]} | #{values.join(' | ')} |\n"
    end
    out << "\n"
    out
  end

  replace_block = lambda do |text, marker, new_content|
    begin_tag = "<!-- BEGIN #{marker} -->"
    end_tag   = "<!-- END #{marker} -->"
    re = /#{Regexp.escape(begin_tag)}.*?#{Regexp.escape(end_tag)}/m
    abort "marker #{begin_tag} not found in README" unless text.match?(re)
    text.sub(re, "#{begin_tag}#{new_content}#{end_tag}")
  end

  readme = File.read(README_PATH)
  readme = replace_block.call(readme, "push_pull", build_push_pull.call)
  readme = replace_block.call(readme, "req_rep",   build_req_rep.call)
  File.write(README_PATH, readme)
  puts "Updated #{README_PATH} (most recent value per cell across #{rows.map { |r| r[:run_id] }.uniq.size} runs)"
  exit 0
end

# ---- default: regression report ----

rows.select! { |r| r[:pattern] == options[:pattern] } if options[:pattern]

# Preserve insertion order (= chronological) rather than sorting
# alphabetically — named run IDs (e.g. "baseline-append") would
# otherwise sort after ISO timestamps.
run_ids = rows.map { |r| r[:run_id] }.uniq.last(options[:runs])

if run_ids.size < 2
  abort "Need at least 2 runs to compare. Found #{run_ids.size}."
end

# Group by measurement key
by_key = Hash.new { |h, k| h[k] = {} }
rows.each do |r|
  next unless run_ids.include?(r[:run_id])
  key = [r[:pattern], r[:transport], r[:peers], r[:msg_size]]
  by_key[key][r[:run_id]] = r
end

# ANSI helpers
RED    = "\e[31m"
GREEN  = "\e[32m"
YELLOW = "\e[33m"
DIM    = "\e[2m"
BOLD   = "\e[1m"
RESET  = "\e[0m"

def format_si(value)
  case
  when value >= 1e9
    "%.1fG"  % (value / 1e9)
  when value >= 1e6
    "%.1fM"  % (value / 1e6)
  when value >= 1e3
    "%.1fk"  % (value / 1e3)
  else                    "%.0f"   % value
  end
end

def format_mbps(value)
  case
  when value >= 1_000_000
    "%.1f TB/s" % (value / 1_000_000)
  when value >= 1_000
    "%.1f GB/s" % (value / 1_000)
  else                         "%.1f MB/s" % value
  end
end

def format_size(bytes)
  case
  when bytes >= 1024
    "#{bytes / 1024}KB"
  else                    "#{bytes}B"
  end
end

threshold    = options[:threshold]
base_run     = run_ids.first   # oldest of the window
latest_run   = run_ids.last
regressions  = []
improvements = []
trends       = []
stable_count = 0

by_key.sort.each do |key, runs|
  base   = runs[base_run]
  latest = runs[latest_run]
  next unless base && latest

  pattern, transport, peers, msg_size = key
  peer_label = "#{peers} peer#{'s' if peers > 1}"

  [:msgs_s, :mbps].each do |metric|
    old_val = base[metric]
    new_val = latest[metric]
    next if old_val.nil? || old_val.zero?

    fmt     = metric == :msgs_s ? method(:format_si) : method(:format_mbps)
    delta   = ((new_val - old_val) / old_val * 100).round(1)
    row     = { pattern: pattern, transport: transport, peers: peer_label,
                size: format_size(msg_size), metric: metric,
                old: fmt.(old_val), new: fmt.(new_val), delta: delta }

    if delta <= -threshold
      regressions << row
    elsif delta >= threshold
      improvements << row
    else
      # Check for monotonic trend across all N runs (requires 3+ runs)
      values = run_ids.map { |id| runs[id]&.fetch(metric, nil) }.compact
      if values.size >= 3
        declining  = values.each_cons(2).all? { |a, b| b < a }
        increasing = values.each_cons(2).all? { |a, b| b > a }
        if declining || increasing
          direction = declining ? :down : :up
          trends << row.merge(direction: direction, runs: values.size)
        else
          stable_count += 1
        end
      else
        stable_count += 1
      end
    end
  end
end

total = regressions.size + improvements.size + trends.size + stable_count

span_label = run_ids.size == 2 ? "#{latest_run} vs #{base_run}" :
             "#{latest_run} vs #{base_run} (#{run_ids.size} runs)"
puts "#{BOLD}=== OMQ Benchmark Report (#{span_label}) ===#{RESET}"
puts

if regressions.any?
  puts "#{RED}#{BOLD}REGRESSIONS (>#{threshold}% vs oldest):#{RESET}"
  regressions.each do |r|
    printf "  %-15s %-8s %-9s %5s  %-6s  %10s → %-10s  #{RED}%+.1f%%#{RESET}\n",
           r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], r[:delta]
  end
  puts
end

if improvements.any?
  puts "#{GREEN}#{BOLD}IMPROVEMENTS (>#{threshold}% vs oldest):#{RESET}"
  improvements.each do |r|
    printf "  %-15s %-8s %-9s %5s  %-6s  %10s → %-10s  #{GREEN}%+.1f%%#{RESET}\n",
           r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], r[:delta]
  end
  puts
end

if trends.any?
  puts "#{YELLOW}#{BOLD}TRENDS (monotonic across #{run_ids.size} runs, within ±#{threshold}%):#{RESET}"
  trends.each do |r|
    arrow = r[:direction] == :down ? "↓" : "↑"
    printf "  %-15s %-8s %-9s %5s  %-6s  %10s → %-10s  #{YELLOW}%s %+.1f%%#{RESET}\n",
           r[:pattern], r[:transport], r[:peers], r[:size], r[:metric], r[:old], r[:new], arrow, r[:delta]
  end
  puts
end

if regressions.empty? && improvements.empty? && trends.empty?
  puts "#{DIM}All #{total} measurements stable (±#{threshold}%)#{RESET}"
else
  puts "#{DIM}#{total} measurements total: #{regressions.size} regressions, " \
       "#{improvements.size} improvements, #{trends.size} trends, #{stable_count} stable (±#{threshold}%)#{RESET}"
end

# --all: full table grouped by pattern
if options[:all]
  puts
  puts "#{BOLD}=== Full Results ===#{RESET}"

  by_key.sort.each do |key, runs|
    pattern, transport, peers, msg_size = key
    peer_label = "#{peers} peer#{'s' if peers > 1}"

    printf "\n  %-15s %-8s %-9s %5s", pattern, transport, peer_label, format_size(msg_size)

    [:msgs_s, :mbps].each do |metric|
      values = run_ids.map { |id| runs[id]&.fetch(metric, nil) }
      fmt    = metric == :msgs_s ? method(:format_si) : method(:format_mbps)

      printf "  %-6s", metric
      values.each { |v| printf "  %10s", v ? fmt.(v) : "--" }

      base_val   = values.first
      latest_val = values.last
      if base_val && latest_val && !base_val.zero?
        delta = ((latest_val - base_val) / base_val * 100).round(1)
        color = delta <= -threshold ? RED : delta >= threshold ? GREEN : DIM
        printf "  #{color}%+.1f%%#{RESET}", delta
      end
    end
  end
  puts
  puts
end
