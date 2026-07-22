# Benchmarks

Measured in a Linux VM on a 2018 Intel Mac Mini, Ruby 4.0.2 +YJIT. Each cell
is the fastest of 3 timed rounds (~1 s each) after a calibration warmup, so
transient scheduler/GC jitter is filtered out. Between-run variance on the same
machine is ~5-15 % depending on transport. Treat single-digit deltas across
runs as noise.

### Reading the numbers (\*)

The same `String` payload is reused across every send. No per-message
allocation. The primary metric is **msg/s** (raw send-path throughput,
what the library can actually push through its queues and codec). The
**MB/s\*** figures are nominal. They are `msg/s × msg_size`, which for
inproc overstates real memory bandwidth (inproc passes the `String` by
reference through the engine queue. No bytes are copied). For IPC/TCP
the bytes really do traverse the kernel, so MB/s there is meaningful
within kernel-buffer/loopback limits. Cross-impl comparison is fairer
this way: Ruby's `String#dup` is copy-on-write while Crystal's
`Bytes#dup` is a real memcpy, so a `.dup`-per-send bench would compare
allocator speed rather than send speed.

Regenerate the tables below from the latest run in `results.jsonl`:

```sh
ruby bench/report.rb --update-readme
```

## Throughput (PUSH/PULL, msg/s)

```
┌──────┐       ┌──────┐
│ PUSH │──────→│ PULL │
└──────┘       └──────┘
```

<!-- BEGIN push_pull -->
### 1 peer

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 8 B | 1.64M msg/s / 13.1 MB/s* | 589.4k msg/s / 4.72 MB/s* | 604.1k msg/s / 4.83 MB/s* |
| 32 B | 1.65M msg/s / 52.7 MB/s* | 554.0k msg/s / 17.7 MB/s* | 575.8k msg/s / 18.4 MB/s* |
| 128 B | 1.70M msg/s / 217 MB/s* | 550.4k msg/s / 70.4 MB/s* | 557.7k msg/s / 71.4 MB/s* |
| 512 B | 1.84M msg/s / 943 MB/s* | 431.3k msg/s / 221 MB/s* | 427.9k msg/s / 219 MB/s* |
| 2 KiB | 1.81M msg/s / 3.72 GB/s* | 324.3k msg/s / 664 MB/s* | 318.4k msg/s / 652 MB/s* |
| 8 KiB | 1.84M msg/s / 15.09 GB/s* | 166.1k msg/s / 1.36 GB/s* | 164.3k msg/s / 1.35 GB/s* |
| 32 KiB | 1.83M msg/s / 60.09 GB/s* | 62.9k msg/s / 2.06 GB/s* | 55.5k msg/s / 1.82 GB/s* |
| 128 KiB | 1.82M msg/s / 238.35 GB/s* | 16.0k msg/s / 2.10 GB/s* | 14.4k msg/s / 1.89 GB/s* |
| 512 KiB | 1.83M msg/s / 959.73 GB/s* | 4.9k msg/s / 2.59 GB/s* | 5.2k msg/s / 2.72 GB/s* |

### 3 peers

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 8 B | 1.82M msg/s / 14.6 MB/s* | 630.7k msg/s / 5.05 MB/s* | 595.2k msg/s / 4.76 MB/s* |
| 32 B | 1.83M msg/s / 58.5 MB/s* | 590.6k msg/s / 18.9 MB/s* | 553.6k msg/s / 17.7 MB/s* |
| 128 B | 1.83M msg/s / 235 MB/s* | 569.9k msg/s / 73.0 MB/s* | 536.5k msg/s / 68.7 MB/s* |
| 512 B | 1.82M msg/s / 932 MB/s* | 439.6k msg/s / 225 MB/s* | 410.1k msg/s / 210 MB/s* |
| 2 KiB | 1.80M msg/s / 3.68 GB/s* | 317.7k msg/s / 651 MB/s* | 303.6k msg/s / 622 MB/s* |
| 8 KiB | 1.84M msg/s / 15.04 GB/s* | 164.8k msg/s / 1.35 GB/s* | 157.0k msg/s / 1.29 GB/s* |
| 32 KiB | 1.83M msg/s / 59.91 GB/s* | 56.2k msg/s / 1.84 GB/s* | 52.0k msg/s / 1.70 GB/s* |
| 128 KiB | 1.84M msg/s / 241.75 GB/s* | 15.4k msg/s / 2.02 GB/s* | 14.6k msg/s / 1.91 GB/s* |
| 512 KiB | 1.84M msg/s / 967.27 GB/s* | 4.5k msg/s / 2.35 GB/s* | 5.1k msg/s / 2.67 GB/s* |

<!-- END push_pull -->

## Round-trip latency (REQ/REP, µs)

```
┌─────┐  req   ┌─────┐
│ REQ │───────→│ REP │
│     │←───────│     │
└─────┘  rep   └─────┘
```

Round-trip = one `req.send` + one `req.receive` + matching `rep` ops.
Latency is `1 / msgs_s` converted to µs.

<!-- BEGIN req_rep -->
| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 8 B | 6.51 µs | 36.1 µs | 45.5 µs |
| 32 B | 6.42 µs | 37.1 µs | 46.6 µs |
| 128 B | 6.49 µs | 36.9 µs | 47.1 µs |
| 512 B | 6.49 µs | 40.0 µs | 49.5 µs |
| 2 KiB | 6.55 µs | 41.8 µs | 52.1 µs |
| 8 KiB | 6.59 µs | 48.2 µs | 56.5 µs |
| 32 KiB | 6.60 µs | 59.6 µs | 69.8 µs |
| 128 KiB | 6.51 µs | 111 µs | 130 µs |
| 512 KiB | 6.60 µs | 421 µs | 446 µs |

<!-- END req_rep -->

## io_uring

With `liburing-dev` installed, io-event uses io_uring instead of epoll.
Inproc throughput jumps significantly. IPC and TCP are within variance.

```sh
# Debian/Ubuntu
sudo apt install liburing-dev
gem pristine io-event
```

## Running

```sh
# Full suite (one run_id shared across patterns for cross-pattern comparison)
RUN_ID=$(date +%Y-%m-%dT%H:%M:%S)
for d in push_pull req_rep router_dealer pub_sub
do
  OMQ_BENCH_RUN_ID=$RUN_ID bundle exec ruby --yjit bench/$d/omq.rb
done

# Regression report (latest vs previous run)
bundle exec ruby bench/report.rb

# Regenerate README tables from the latest run
bundle exec ruby bench/report.rb --update-readme

# Full comparison table
bundle exec ruby bench/report.rb --all
```
