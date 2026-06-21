# frozen_string_literal: true

# SVG chart generation helpers, ported from OMQ.rs Python chart infra.
# Produces multi-axis line charts: CPU % (left, linear), throughput (inner
# right, log), msg/s (outer right, log).

module ChartHelper
  module_function


  def fmt_size(bytes)
    if bytes >= 1024 * 1024
      "#{bytes / (1024 * 1024)} MiB"
    elsif bytes >= 1024
      "#{bytes / 1024} KiB"
    else
      "#{bytes} B"
    end
  end


  def fmt_cpu(val)
    "#{val.to_i == val ? val.to_i : val}%"
  end


  def fmt_tput(mb)
    if mb >= 1024
      v = mb / 1024.0
      v < 10 ? "%.1f GB/s" % v : "%.0f GB/s" % v
    elsif mb >= 10
      "%.0f MB/s" % mb
    else
      "%.1f MB/s" % mb
    end
  end


  def fmt_msgs(msgs_s)
    if msgs_s >= 1_000_000
      v = msgs_s / 1_000_000.0
      v < 10 ? "%.1fM" % v : "%.0fM" % v
    elsif msgs_s >= 1_000
      "%.0fK" % (msgs_s / 1_000.0)
    else
      "%.0f" % msgs_s
    end
  end


  def log_ticks(data_min, data_max)
    data_min = 1.0 if data_min <= 0
    data_max = data_min * 10 if data_max <= data_min

    steps = [1, 2, 5]

    prev_125 = ->(v) {
      exp = Math.log10(v).floor
      steps.reverse_each do |s|
        candidate = s * 10**exp
        return candidate if candidate <= v
      end
      10**exp
    }

    next_125 = ->(v) {
      exp = Math.log10(v).floor
      steps.each do |s|
        candidate = s * 10**exp
        return candidate if candidate >= v
      end
      10**(exp + 1)
    }

    lo = prev_125.call(data_min)
    hi = next_125.call(data_max)

    axis_min = Math.log10(lo)
    axis_max = Math.log10(hi)

    ticks = []
    (axis_min.floor..axis_max.ceil).each do |e|
      [1, 2, 5].each do |s|
        v = s * 10**e
        ticks << v if v >= lo && v <= hi
      end
    end

    [axis_min, axis_max, ticks]
  end


  def cpu_ticks(data_max)
    data_max = 100.0 if data_max <= 0
    candidates = [50, 100, 200, 400, 500, 800, 1000]
    ceil = candidates.find { |c| c >= data_max } || (data_max / 100.0).ceil * 100
    step = ceil <= 400 ? 50 : 100
    ticks = (step..ceil.to_i).step(step).to_a
    [ceil.to_f, ticks]
  end


  def read_proc_cpu(pid)
    fields = File.read("/proc/#{pid}/stat").split
    utime  = fields[13].to_i
    stime  = fields[14].to_i
    clk_tck = 100
    (utime + stime).to_f / clk_tck
  rescue Errno::ENOENT, Errno::ESRCH
    0.0
  end


  def detect_hardware
    model = nil
    cores = 0

    if File.exist?("/proc/cpuinfo")
      File.foreach("/proc/cpuinfo") do |line|
        if line.start_with?("model name")
          model ||= line.split(":", 2).last.strip
        end
        cores += 1 if line.start_with?("processor")
      end
    end

    governor = nil
    gov_path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"
    governor = File.read(gov_path).strip if File.exist?(gov_path)

    turbo = nil
    no_turbo_path = "/sys/devices/system/cpu/intel_pstate/no_turbo"
    boost_path    = "/sys/devices/system/cpu/cpufreq/boost"
    if File.exist?(no_turbo_path)
      turbo = File.read(no_turbo_path).strip == "1" ? "turbo off" : "turbo on"
    elsif File.exist?(boost_path)
      turbo = File.read(boost_path).strip == "0" ? "turbo off" : "turbo on"
    end

    hw_file = File.join(__dir__, "..", ".chart_hw")
    prefix = nil
    postfix = nil
    if File.exist?(hw_file)
      File.foreach(hw_file) do |line|
        k, v = line.strip.split("=", 2)
        case k
        when "prefix" then prefix = v
        when "postfix" then postfix = v
        end
      end
    end

    prefix  = ENV.fetch("OMQ_HW_PREFIX", prefix)
    postfix = ENV.fetch("OMQ_HW_POSTFIX", postfix)

    parts = []
    parts << prefix if prefix
    parts << model if model
    parts << "#{cores} cores" if cores > 0
    parts << "#{governor} governor" if governor
    parts << turbo if turbo
    parts << postfix if postfix

    parts.empty? ? nil : parts.join(", ")
  end


  # Generates a single-panel SVG chart with 3 y-axes.
  #
  # series: { "ruby" => { 1024 => { msgs_s:, mbps:, cpu_pct: }, ... }, ... }
  # colors: { "ruby" => "#eab308", "rust" => "#dc2626" }
  # labels: { "ruby" => "pure Ruby + YJIT", "rust" => "Rust (omq-tokio)" }
  # series_order: ["ruby", "rust"] (draw order)
  #
  def generate_single_panel_svg(title:, hw_label:, sizes:, series:, colors:, labels:, series_order:)
    x_left   = 90
    x_right  = 700
    x_right2 = 780
    plot_w   = x_right - x_left

    top_margin = hw_label ? 50 : 36
    panel_h    = 365
    x_label_h  = 20
    legend_h   = 40
    bottom_pad  = 30
    right_pad   = 15

    svg_h = top_margin + panel_h + x_label_h + legend_h + bottom_pad
    svg_w = x_right2 + 80 + right_pad
    mid_x = (x_left + x_right) / 2.0

    n = sizes.length
    return "" if n < 2

    xs = (0...n).map { |i| x_left + i * plot_w.to_f / (n - 1) }

    y_top = top_margin
    y_bot = y_top + panel_h
    plot_h = panel_h

    cpu_data_max = series.values.flat_map { |sd| sd.values.map { |d| d[:cpu_pct] } }.max || 100
    cpu_ceil, cpu_tick_vals = cpu_ticks([cpu_data_max, 100].max)

    all_mbps = series.values.flat_map { |sd| sd.values.map { |d| d[:mbps] }.select(&:positive?) }
    tp_min_log, tp_max_log, tp_tick_vals = log_ticks(
      all_mbps.min || 1, all_mbps.max || 100
    )

    all_msgs = series.values.flat_map { |sd| sd.values.map { |d| d[:msgs_s] }.select(&:positive?) }
    ms_min_log, ms_max_log, ms_tick_vals = log_ticks(
      all_msgs.min || 1, all_msgs.max || 1000
    )

    y_cpu = ->(v) {
      frac = [[v / cpu_ceil, 1.0].min, 0.0].max
      y_bot - frac * plot_h
    }

    y_tput = ->(v) {
      return y_bot if v <= 0
      frac = [[(Math.log10(v) - tp_min_log) / (tp_max_log - tp_min_log), 1.0].min, 0.0].max
      y_bot - frac * plot_h
    }

    y_msgs = ->(v) {
      return y_bot if v <= 0
      frac = [[(Math.log10(v) - ms_min_log) / (ms_max_log - ms_min_log), 1.0].min, 0.0].max
      y_bot - frac * plot_h
    }

    lines = []
    lines << %(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 #{svg_w} #{svg_h}" font-family="system-ui, -apple-system, sans-serif">)
    lines << %(  <rect width="#{svg_w}" height="#{svg_h}" fill="white"/>)

    title_y = hw_label ? 20 : 24
    lines << %(  <text x="#{mid_x}" y="#{title_y}" text-anchor="middle" fill="#111827" font-size="13" font-weight="700">#{title}</text>)

    if hw_label
      lines << %(  <text x="#{mid_x}" y="#{title_y + 16}" text-anchor="middle" fill="#9ca3af" font-size="10">#{hw_label}</text>)
    end

    cpu_tick_vals.each do |val|
      yy = y_cpu.call(val)
      lines << %(  <line x1="#{x_left}" y1="#{f yy}" x2="#{x_right}" y2="#{f yy}" stroke="#e5e7eb" stroke-width="1"/>)
      lines << %(  <text x="#{x_left - 8}" y="#{f yy}" text-anchor="end" dominant-baseline="middle" fill="#374151" font-size="10">#{fmt_cpu(val)}</text>)
    end

    tp_tick_vals.each do |mb|
      yy = y_tput.call(mb)
      lines << %(  <line x1="#{x_left}" y1="#{f yy}" x2="#{x_right}" y2="#{f yy}" stroke="#e5e7eb" stroke-width="1" stroke-dasharray="3,6"/>)
      lines << %(  <text x="#{x_right + 8}" y="#{f yy}" text-anchor="start" dominant-baseline="middle" fill="#6b7280" font-size="10">#{fmt_tput(mb)}</text>)
    end

    ms_tick_vals.each do |ms|
      yy = y_msgs.call(ms)
      lines << %(  <text x="#{x_right2 + 8}" y="#{f yy}" text-anchor="start" dominant-baseline="middle" fill="#9ca3af" font-size="10">#{fmt_msgs(ms)}/s</text>)
    end

    xs.each do |x|
      lines << %(  <line x1="#{f x}" y1="#{y_top}" x2="#{f x}" y2="#{y_bot}" stroke="#e5e7eb" stroke-width="1"/>)
    end

    lines << %(  <line x1="#{x_left}" y1="#{y_top}" x2="#{x_left}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)
    lines << %(  <line x1="#{x_right}" y1="#{y_top}" x2="#{x_right}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)
    lines << %(  <line x1="#{x_left}" y1="#{y_bot}" x2="#{x_right}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)
    lines << %(  <line x1="#{x_right2}" y1="#{y_top}" x2="#{x_right2}" y2="#{y_bot}" stroke="#d1d5db" stroke-width="1"/>)

    mid_y = (y_top + y_bot) / 2.0
    lines << %(  <text x="40" y="#{f mid_y}" text-anchor="middle" fill="#374151" font-size="10" font-weight="600" transform="rotate(-90,40,#{f mid_y})">CPU %</text>)

    present = series_order.select { |k| series.key?(k) }

    present.each do |name|
      sd     = series[name]
      active = (0...n).select { |i| sd.key?(sizes[i]) }.map { |i| [i, sizes[i]] }
      next if active.empty?

      pts = active.map { |i, s| "#{f xs[i]},#{f y_cpu.call(sd[s][:cpu_pct])}" }.join(" ")
      lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="2" stroke-dasharray="2,3"/>)
    end

    present.each do |name|
      sd     = series[name]
      active = (0...n).select { |i| sd.key?(sizes[i]) }.map { |i| [i, sizes[i]] }
      next if active.empty?

      pts = active.map { |i, s| "#{f xs[i]},#{f y_tput.call(sd[s][:mbps])}" }.join(" ")
      lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>)

      active.each do |i, s|
        yy = y_tput.call(sd[s][:mbps])
        lines << %(  <circle cx="#{f xs[i]}" cy="#{f yy}" r="3" fill="#{colors[name]}" stroke="white" stroke-width="1"/>)
      end
    end

    present.each do |name|
      sd     = series[name]
      active = (0...n).select { |i| sd.key?(sizes[i]) }.map { |i| [i, sizes[i]] }
      next if active.empty?

      pts = active.map { |i, s| "#{f xs[i]},#{f y_msgs.call(sd[s][:msgs_s])}" }.join(" ")
      lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="1.5" stroke-dasharray="5,3"/>)
    end

    sizes.each_with_index do |s, i|
      lines << %(  <text x="#{f xs[i]}" y="#{y_bot + 14}" text-anchor="middle" fill="#374151" font-size="8.5">#{fmt_size(s)}</text>)
    end

    leg_y = y_bot + x_label_h + 14
    item_w = plot_w.to_f / [present.size, 1].max

    present.each_with_index do |name, i|
      lx = x_left + i * item_w
      c  = colors[name]

      lines << %(  <line x1="#{f lx}" y1="#{leg_y}" x2="#{f lx + 14}" y2="#{leg_y}" stroke="#{c}" stroke-width="2.5"/>)
      lines << %(  <circle cx="#{f lx + 7}" cy="#{leg_y}" r="2.5" fill="#{c}"/>)
      lines << %(  <text x="#{f lx + 20}" y="#{leg_y + 4}" fill="#374151" font-size="11" font-weight="500">#{labels[name]}</text>)
    end

    footer_y = leg_y + 20
    lines << %(  <line x1="#{x_left + 10}" y1="#{footer_y}" x2="#{x_left + 24}" y2="#{footer_y}" stroke="#6b7280" stroke-width="2" stroke-dasharray="2,3" opacity="0.7"/>)
    lines << %(  <text x="#{x_left + 30}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">CPU % (left)</text>)

    lines << %(  <line x1="#{x_left + 155}" y1="#{footer_y}" x2="#{x_left + 169}" y2="#{footer_y}" stroke="#6b7280" stroke-width="2.5"/>)
    lines << %(  <circle cx="#{x_left + 162}" cy="#{footer_y}" r="2" fill="#6b7280"/>)
    lines << %(  <text x="#{x_left + 175}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">MB/s (inner right)</text>)

    lines << %(  <line x1="#{x_left + 330}" y1="#{footer_y}" x2="#{x_left + 344}" y2="#{footer_y}" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3"/>)
    lines << %(  <text x="#{x_left + 350}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">msg/s (outer right)</text>)

    lines << %(</svg>)
    lines.join("\n") + "\n"
  end


  # Generates a latency chart (REQ/REP style) with p50 and p99 lines.
  #
  # series: { "ruby" => { 1024 => { p50:, p99:, p999:, max: }, ... }, ... }
  #
  def generate_latency_panel_svg(title:, hw_label:, sizes:, series:, colors:, labels:, series_order:)
    x_left   = 90
    x_right  = 700
    x_right2 = 780
    plot_w   = x_right - x_left

    top_margin = hw_label ? 50 : 36
    panel_h    = 365
    x_label_h  = 20
    legend_h   = 40
    bottom_pad = 30
    right_pad  = 15

    svg_h = top_margin + panel_h + x_label_h + legend_h + bottom_pad
    svg_w = x_right2 + 80 + right_pad
    mid_x = (x_left + x_right) / 2.0

    n = sizes.length
    return "" if n < 2

    xs = (0...n).map { |i| x_left + i * plot_w.to_f / (n - 1) }

    y_top  = top_margin
    y_bot  = y_top + panel_h
    plot_h = panel_h

    has_cpu = series.values.any? { |sd| sd.values.any? { |d| (d[:cpu_pct] || 0) > 0 } }

    all_latencies = series.values.flat_map { |sd| sd.values.flat_map { |d| [d[:p50], d[:p99]] } }.select(&:positive?)
    lat_min_log, lat_max_log, lat_tick_vals = log_ticks(
      all_latencies.min || 1, all_latencies.max || 1000
    )

    y_lat = ->(v) {
      return y_bot if v <= 0
      frac = [[(Math.log10(v) - lat_min_log) / (lat_max_log - lat_min_log), 1.0].min, 0.0].max
      y_bot - frac * plot_h
    }

    if has_cpu
      cpu_data_max = series.values.flat_map { |sd| sd.values.map { |d| d[:cpu_pct] || 0 } }.max || 100
      cpu_ceil, cpu_tick_vals = cpu_ticks([cpu_data_max, 100].max)

      y_cpu = ->(v) {
        frac = [[v / cpu_ceil, 1.0].min, 0.0].max
        y_bot - frac * plot_h
      }
    end

    lines = []
    lines << %(<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 #{svg_w} #{svg_h}" font-family="system-ui, -apple-system, sans-serif">)
    lines << %(  <rect width="#{svg_w}" height="#{svg_h}" fill="white"/>)

    title_y = hw_label ? 20 : 24
    lines << %(  <text x="#{mid_x}" y="#{title_y}" text-anchor="middle" fill="#111827" font-size="13" font-weight="700">#{title}</text>)

    if hw_label
      lines << %(  <text x="#{mid_x}" y="#{title_y + 16}" text-anchor="middle" fill="#9ca3af" font-size="10">#{hw_label}</text>)
    end

    lat_tick_vals.each do |val|
      yy = y_lat.call(val)
      lines << %(  <line x1="#{x_left}" y1="#{f yy}" x2="#{x_right}" y2="#{f yy}" stroke="#e5e7eb" stroke-width="1"/>)
      lines << %(  <text x="#{x_left - 8}" y="#{f yy}" text-anchor="end" dominant-baseline="middle" fill="#374151" font-size="10">#{fmt_latency(val)}</text>)
    end

    if has_cpu
      cpu_tick_vals.each do |val|
        yy = y_cpu.call(val)
        lines << %(  <text x="#{x_right + 8}" y="#{f yy}" text-anchor="start" dominant-baseline="middle" fill="#9ca3af" font-size="10">#{fmt_cpu(val)}</text>)
      end
    end

    xs.each do |x|
      lines << %(  <line x1="#{f x}" y1="#{y_top}" x2="#{f x}" y2="#{y_bot}" stroke="#e5e7eb" stroke-width="1"/>)
    end

    lines << %(  <line x1="#{x_left}" y1="#{y_top}" x2="#{x_left}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)
    lines << %(  <line x1="#{x_right}" y1="#{y_top}" x2="#{x_right}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)
    lines << %(  <line x1="#{x_left}" y1="#{y_bot}" x2="#{x_right}" y2="#{y_bot}" stroke="#9ca3af" stroke-width="1.5"/>)

    mid_y = (y_top + y_bot) / 2.0
    lines << %(  <text x="40" y="#{f mid_y}" text-anchor="middle" fill="#374151" font-size="10" font-weight="600" transform="rotate(-90,40,#{f mid_y})">Latency (µs)</text>)

    present = series_order.select { |k| series.key?(k) }

    if has_cpu
      present.each do |name|
        sd     = series[name]
        active = (0...n).select { |i| sd.key?(sizes[i]) && (sd[sizes[i]][:cpu_pct] || 0) > 0 }.map { |i| [i, sizes[i]] }
        next if active.empty?

        pts = active.map { |i, s| "#{f xs[i]},#{f y_cpu.call(sd[s][:cpu_pct])}" }.join(" ")
        lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="2" stroke-dasharray="2,3"/>)
      end
    end

    present.each do |name|
      sd     = series[name]
      active = (0...n).select { |i| sd.key?(sizes[i]) }.map { |i| [i, sizes[i]] }
      next if active.empty?

      pts = active.map { |i, s| "#{f xs[i]},#{f y_lat.call(sd[s][:p50])}" }.join(" ")
      lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>)

      active.each do |i, s|
        yy = y_lat.call(sd[s][:p50])
        lines << %(  <circle cx="#{f xs[i]}" cy="#{f yy}" r="3" fill="#{colors[name]}" stroke="white" stroke-width="1"/>)
      end
    end

    present.each do |name|
      sd     = series[name]
      active = (0...n).select { |i| sd.key?(sizes[i]) }.map { |i| [i, sizes[i]] }
      next if active.empty?

      pts = active.map { |i, s| "#{f xs[i]},#{f y_lat.call(sd[s][:p99])}" }.join(" ")
      lines << %(  <polyline points="#{pts}" fill="none" stroke="#{colors[name]}" stroke-width="1.5" stroke-dasharray="5,3"/>)
    end

    sizes.each_with_index do |s, i|
      lines << %(  <text x="#{f xs[i]}" y="#{y_bot + 14}" text-anchor="middle" fill="#374151" font-size="8.5">#{fmt_size(s)}</text>)
    end

    leg_y  = y_bot + x_label_h + 14
    item_w = plot_w.to_f / [present.size, 1].max

    present.each_with_index do |name, i|
      lx = x_left + i * item_w
      c  = colors[name]

      lines << %(  <line x1="#{f lx}" y1="#{leg_y}" x2="#{f lx + 14}" y2="#{leg_y}" stroke="#{c}" stroke-width="2.5"/>)
      lines << %(  <circle cx="#{f lx + 7}" cy="#{leg_y}" r="2.5" fill="#{c}"/>)
      lines << %(  <text x="#{f lx + 20}" y="#{leg_y + 4}" fill="#374151" font-size="11" font-weight="500">#{labels[name]}</text>)
    end

    footer_y = leg_y + 20
    if has_cpu
      lines << %(  <line x1="#{x_left + 10}" y1="#{footer_y}" x2="#{x_left + 24}" y2="#{footer_y}" stroke="#6b7280" stroke-width="2" stroke-dasharray="2,3" opacity="0.7"/>)
      lines << %(  <text x="#{x_left + 30}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">CPU % (right)</text>)

      lines << %(  <line x1="#{x_left + 155}" y1="#{footer_y}" x2="#{x_left + 169}" y2="#{footer_y}" stroke="#6b7280" stroke-width="2.5"/>)
      lines << %(  <circle cx="#{x_left + 162}" cy="#{footer_y}" r="2" fill="#6b7280"/>)
      lines << %(  <text x="#{x_left + 175}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">p50 (median)</text>)

      lines << %(  <line x1="#{x_left + 310}" y1="#{footer_y}" x2="#{x_left + 324}" y2="#{footer_y}" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3"/>)
      lines << %(  <text x="#{x_left + 330}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">p99</text>)
    else
      lines << %(  <line x1="#{x_left + 10}" y1="#{footer_y}" x2="#{x_left + 24}" y2="#{footer_y}" stroke="#6b7280" stroke-width="2.5"/>)
      lines << %(  <circle cx="#{x_left + 17}" cy="#{footer_y}" r="2" fill="#6b7280"/>)
      lines << %(  <text x="#{x_left + 30}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">p50 (median)</text>)

      lines << %(  <line x1="#{x_left + 155}" y1="#{footer_y}" x2="#{x_left + 169}" y2="#{footer_y}" stroke="#6b7280" stroke-width="1.5" stroke-dasharray="5,3"/>)
      lines << %(  <text x="#{x_left + 175}" y="#{footer_y + 4}" fill="#6b7280" font-size="10">p99</text>)
    end

    lines << %(</svg>)
    lines.join("\n") + "\n"
  end


  def fmt_latency(us)
    if us >= 1000
      "%.1f ms" % (us / 1000.0)
    elsif us >= 100
      "%.0f µs" % us
    elsif us >= 10
      "%.0f µs" % us
    else
      "%.1f µs" % us
    end
  end


  def f(v)
    "%.1f" % v
  end
end
