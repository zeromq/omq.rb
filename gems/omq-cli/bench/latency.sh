#!/bin/sh
#
# omq latency benchmark (REQ/REP roundtrip)
#
# Usage: sh bench/omq/latency.sh [count]
#
set -u

OMQ="ruby --yjit -Ilib exe/omq"
N=${1:-1000}
PORT=18200

echo "omq latency benchmark — $N roundtrips"
echo

for transport in tcp ipc; do
  if [ "$transport" = "tcp" ]; then
    ADDR="tcp://:$PORT"
    CONN="tcp://localhost:$PORT"
    PORT=$((PORT + 1))
  else
    ADDR="ipc://@omq_bench_lat_$$"
    CONN="$ADDR"
  fi

  # Start responder
  $OMQ rep -b "$ADDR" -D "pong" -n $N -q 2>/dev/null &
  REP_PID=$!
  sleep 0.5

  # Run REQ ping loop
  START=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')
  $OMQ req -c "$CONN" -D "ping" -i 0 -n $N -q 2>/dev/null
  END=$(ruby -e 'puts Process.clock_gettime(Process::CLOCK_MONOTONIC)')
  wait $REP_PID 2>/dev/null

  ELAPSED=$(ruby -e "puts ($END - $START).round(3)")
  LATENCY=$(ruby -e "puts (($END - $START) / $N * 1_000_000).round(1)")
  RATE=$(ruby -e "puts ($N.to_f / ($END - $START)).round(0)")

  echo "  $transport: ${LATENCY} us/roundtrip (${RATE} roundtrips/s, ${N} in ${ELAPSED}s)"
done
