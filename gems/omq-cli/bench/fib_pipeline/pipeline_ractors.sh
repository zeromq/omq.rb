#!/bin/sh
#
# omq pipeline benchmark (Ractor-parallel)
#
# Topology:
#
#   +----------+     +----------+     +------+
#   | producer |-IPC-| pipe -P4 |-IPC-| sink |
#   | PUSH     |     | Ractors  |     | PULL |
#   +----------+     +----------+     +------+
#
# Same as pipeline.sh but uses a single `omq pipe -P` process
# with Ractor workers instead of 4 separate pipe processes.
#
# Usage: sh bench/cli/pipeline_ractors.sh [count] [fib_max] [workers]
#
set -u

BENCH_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$BENCH_DIR/../../.." && pwd)
OMQ="ruby --yjit -I$ROOT_DIR/lib $ROOT_DIR/exe/omq"
N=${1:-1000}
FIB_MAX=${2:-20}
WORKERS=${3:-4}
ID=$$
WORK="ipc://@omq_bench_work_$ID"
SINK="ipc://@omq_bench_sink_$ID"

echo "omq pipeline benchmark (Ractors) — $N messages, $WORKERS workers, fib(1..$FIB_MAX)"
echo

# ── Sink: bind first so workers can connect ──────────────────

START=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')

$OMQ pull --bind $SINK --transient 2>/dev/null \
| awk '{ s += $1 } END { print s }' > "/tmp/omq_bench_sum_$ID" &
SINK_PID=$!

# ── Producer: bind and generate work ─────────────────────────

ruby --yjit -e "
ints = (1..$FIB_MAX).cycle
$N.times { puts ints.next }
" | $OMQ push --bind $WORK --linger 5 2>/dev/null &
PRODUCER_PID=$!

# ── Workers: single pipe process with -P Ractors ────────────

$OMQ pipe -c $WORK -c $SINK -P $WORKERS \
  -r"$BENCH_DIR/fib.rb" \
  -e '[fib(Integer(it.first)).to_s]' \
  --transient -t 1 2>/dev/null &
PIPE_PID=$!

# ── Wait for sink to finish ─────────────────────────────────

wait $SINK_PID

END=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')

ELAPSED=$(ruby -e "puts ($END - $START).round(3)")
RATE=$(ruby -e "puts ($N.to_f / ($END - $START)).round(1)")

SUM=$(cat "/tmp/omq_bench_sum_$ID")
echo "  $WORKERS Ractor workers: $RATE msg/s ($N messages in ${ELAPSED}s, sum=$SUM)"

# Clean up
rm -f "/tmp/omq_bench_sum_$ID"
kill $PRODUCER_PID $PIPE_PID 2>/dev/null
wait 2>/dev/null
