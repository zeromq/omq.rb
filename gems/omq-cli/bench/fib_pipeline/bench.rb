# frozen_string_literal: true

# Combined fib pipeline benchmark: multi-process vs Ractors.
#
# Runs both modes across increasing fib complexity,
# plots msg/s comparison, and writes README.md.
#
# Usage: ruby --yjit bench/fib_pipeline/bench.rb

$VERBOSE = nil

require "unicode_plot"

BENCH_DIR = __dir__
ROOT_DIR  = File.expand_path("../..", BENCH_DIR)
OMQ       = "ruby --yjit -I#{ROOT_DIR}/lib #{ROOT_DIR}/exe/omq"
WORKERS   = 4

# fib_max → message count (fewer messages for heavier work)
RUNS = {
  10 => 100_000,
  15 => 100_000,
  20 => 100_000,
  23 => 50_000,
  25 => 10_000,
  27 => 10_000,
  29 => 10_000,
}

def run_pipeline(mode, n, fib_max)
  id   = "#{$$}_#{mode}_#{fib_max}"
  work = "ipc://@omq_bench_work_#{id}"
  sink = "ipc://@omq_bench_sink_#{id}"
  sum_file = "/tmp/omq_bench_sum_#{id}"

  producer_cmd = "ruby --yjit -e \"ints = (1..#{fib_max}).cycle; #{n}.times { puts ints.next }\" " \
                 "| #{OMQ} push --bind #{work} --linger 5 2>/dev/null"

  fib_expr = "-r#{BENCH_DIR}/fib.rb -e '[fib(Integer(it.first)).to_s]'"

  case mode
  when :multiprocess
    worker_cmd = "seq #{WORKERS} | xargs -P #{WORKERS} -I{} " \
                 "#{OMQ} pipe -c #{work} -c #{sink} #{fib_expr} --transient -t 1 2>/dev/null"
  when :ractors
    worker_cmd = "#{OMQ} pipe -c #{work} -c #{sink} -P #{WORKERS} #{fib_expr} --transient -t 1 2>/dev/null"
  end

  sink_cmd = "#{OMQ} pull --bind #{sink} --transient 2>/dev/null | wc -l > #{sum_file}"

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  sink_pid    = spawn(sink_cmd, [:out, :err] => "/dev/null")
  sleep 0.2
  producer_pid = spawn(producer_cmd)
  worker_pid   = spawn(worker_cmd)

  Process.wait(sink_pid)
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

  Process.kill("TERM", producer_pid) rescue nil
  Process.kill("TERM", worker_pid) rescue nil
  Process.wait(producer_pid) rescue nil
  Process.wait(worker_pid) rescue nil
  File.delete(sum_file) rescue nil

  msgs_s = n / elapsed
  printf "  %-14s fib(1..%-2d)  %8.0f msg/s  (%d msgs in %.1fs)\n",
         mode, fib_max, msgs_s, n, elapsed
  msgs_s
end

jit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "+YJIT" : "no JIT"
puts "fib pipeline | Ruby #{RUBY_VERSION} (#{jit}) | #{WORKERS} workers"
puts

multi_results  = []
ractor_results = []

RUNS.each do |fib_max, n|
  multi_results  << run_pipeline(:multiprocess, n, fib_max)
  ractor_results << run_pipeline(:ractors, n, fib_max)
  puts
end

# -- plot ------------------------------------------------------------------

x = RUNS.keys.map(&:to_f)

log_multi  = multi_results.map  { |v| Math.log10(v) }
log_ractor = ractor_results.map { |v| Math.log10(v) }

all_vals = multi_results + ractor_results
log_min  = Math.log10(all_vals.min).floor
log_max  = Math.log10(all_vals.max).ceil

plot = UnicodePlot.lineplot(x, log_multi,
                            title:  "fib pipeline: multi-process vs Ractors (#{WORKERS} workers)",
                            xlabel: "fib(1..N)",
                            ylabel: "msgs/s",
                            ylim:   [log_min, log_max],
                            width:  60,
                            height: 15)
UnicodePlot.lineplot!(plot, x, log_ractor)

n_rows = plot.n_rows
(log_min..log_max).each do |decade|
  fraction = (decade - log_min).to_f / (log_max - log_min)
  row      = n_rows - 1 - (fraction * (n_rows - 1)).round
  label = case
          when decade >= 6
            "%.0fM" % (10**decade / 1e6)
          when decade >= 3
            "%.0fk" % (10**decade / 1e3)
          else                   "%.0f" % 10**decade
          end
  plot.annotate_row!(:l, row, label)
end

# Label each line at its endpoint
series_info = [
  { name: "multi-process", last_log_y: log_multi.last,  bullet: "▪" },
  { name: "Ractors",       last_log_y: log_ractor.last, bullet: "▫" },
]
used_rows = {}
series_info.each do |s|
  fraction = (s[:last_log_y] - log_min) / (log_max - log_min)
  row      = (n_rows - 1 - (fraction * (n_rows - 1)).round).clamp(0, n_rows - 1)
  row -= 1 while row >= 0 && used_rows[row]
  row = row.clamp(0, n_rows - 1)
  used_rows[row] = true
  plot.annotate_row!(:r, row, "#{s[:bullet]} #{s[:name]}")
end

plot.annotate!(:bl, "")
plot.annotate!(:br, "")

puts
plot.render($stdout)
puts

# -- write README ----------------------------------------------------------

sio = StringIO.new
plot.render(sio, color: false)

readme = +"# fib pipeline\n\n"
readme << "Ruby #{RUBY_VERSION} (#{jit}) | #{WORKERS} workers | #{Time.now.strftime('%Y-%m-%d')}\n\n"

readme << "```\n"
RUNS.each_with_index do |(fib_max, n), i|
  readme << "  fib(1..%-2d)  multi-process: %6.0f msg/s  Ractors: %6.0f msg/s  (%d msgs)\n" %
            [fib_max, multi_results[i], ractor_results[i], n]
end
readme << "```\n\n"

readme << "```\n#{sio.string}```\n"

File.write(File.join(BENCH_DIR, "README.md"), readme)
puts "(wrote #{BENCH_DIR}/README.md)"
