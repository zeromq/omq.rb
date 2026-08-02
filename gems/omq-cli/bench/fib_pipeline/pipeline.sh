#!/bin/sh
#
# omq pipeline benchmark
#
# Topology:
#
#   +----------+     +--------+     +------+
#   | producer |-IPC-| worker |-IPC-| sink |
#   | PUSH     |     | pipe×4 |     | PULL |
#   +----------+     +--------+     +------+
#
# Producer sends N integers (cycling 1..FIB_MAX).
# Each worker computes fib(n) and forwards the result.
# Sink collects results and prints the sum.
#
# Usage: sh bench/cli/pipeline.sh [count] [fib_max]
#
set -u

BENCH_DIR=$(cd "$(dirname "$0")" && pwd)
ROOT_DIR=$(cd "$BENCH_DIR/../../.." && pwd)
OMQ="ruby --yjit -I$ROOT_DIR/lib $ROOT_DIR/exe/omq"
N=${1:-1000}
FIB_MAX=${2:-20}
WORKERS=4
ID=$$
WORK="ipc://@omq_bench_work_$ID"
SINK="ipc://@omq_bench_sink_$ID"

echo "omq pipeline benchmark — $N messages, $WORKERS workers, fib(1..$FIB_MAX)"
echo

# ── Producer: bind and generate work ─────────────────────────

START=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')

ruby --yjit -e "
ints = (1..$FIB_MAX).cycle
$N.times { puts ints.next }
" | $OMQ push --bind $WORK --linger 5 2>/dev/null &
PRODUCER_PID=$!

# ── Workers: pull → fib → push ──────────────────────────────

seq $WORKERS | xargs -P $WORKERS -I{} \
  $OMQ pipe -c $WORK -c $SINK \
    -r"$BENCH_DIR/fib.rb" \
    -e '[fib(Integer(it.first)).to_s]' \
    --transient -t 1 2>/dev/null & # -t 1: exit if producer is already gone
WORKERS_PID=$!

# ── Sink: pull results ───────────────────────────────────────

$OMQ pull --bind $SINK --transient 2>/dev/null \
| awk '{ s += $1 } END { print s }' > "/tmp/omq_bench_sum_$ID"

# ── Done ─────────────────────────────────────────────────────

END=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')

ELAPSED=$(ruby -e "puts ($END - $START).round(3)")
RATE=$(ruby -e "puts ($N.to_f / ($END - $START)).round(1)")

SUM=$(cat "/tmp/omq_bench_sum_$ID")
echo "  $WORKERS workers: $RATE msg/s ($N messages in ${ELAPSED}s, sum=$SUM)"

# Clean up
rm -f "/tmp/omq_bench_sum_$ID"
kill $PRODUCER_PID $WORKERS_PID 2>/dev/null
wait 2>/dev/null
