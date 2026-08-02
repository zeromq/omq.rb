#!/bin/sh
# -P N: Ractor-based parallel runners for pull, pull+zstd, rep --echo,
# and gather. Each worker owns its own socket pair.

. "$(dirname "$0")/support.sh"

if [ "$OMQ_SYSTEM_BACKEND" = "libzmq" ]; then
  skip "parallel Ractor mode unsupported by $OMQ_SYSTEM_BACKEND backend"
  exit 0
fi

echo "Parallel PULL:"
U=$(ipc_file)
seq 10 | $OMQ push -b $U $T 2>>"$STDERR_LOG" &
PPUSH_PID=$!
wait_for_ipc_bind "$U"
$OMQ pull -c $U -P 2 $T > $TMPDIR/ppull_out.txt 2>>"$STDERR_LOG" &
PPULL_PID=$!
wait $PPULL_PID 2>/dev/null || true
kill $PPUSH_PID 2>/dev/null || true; wait $PPUSH_PID 2>/dev/null || true
PPULL_CONTENT=$(cat $TMPDIR/ppull_out.txt | sort -n | tr '\n' ',')
check "pull -P2 receives all messages" "1,2,3,4,5,6,7,8,9,10," "$PPULL_CONTENT"

echo "Parallel PULL -z:"
if backend_supports_compression; then
  U=$(ipc_file)
  seq 10 | $OMQ push -b $U -z $T 2>>"$STDERR_LOG" &
  PPUSHZ_PID=$!
  wait_for_ipc_bind "$U"
  $OMQ pull -c $U -P 2 -z $T > $TMPDIR/ppullz_out.txt 2>>"$STDERR_LOG" &
  PPULLZ_PID=$!
  wait $PPULLZ_PID 2>/dev/null || true
  kill $PPUSHZ_PID 2>/dev/null || true; wait $PPUSHZ_PID 2>/dev/null || true
  PPULLZ_CONTENT=$(cat $TMPDIR/ppullz_out.txt | sort -n | tr '\n' ',')
  check "pull -P2 -z decompresses correctly" "1,2,3,4,5,6,7,8,9,10," "$PPULLZ_CONTENT"
else
  skip "parallel compression unsupported by $OMQ_SYSTEM_BACKEND backend"
fi

echo "Parallel REP --echo:"
U=$(ipc_file)
$OMQ rep -c $U -P 2 --echo $T > /dev/null 2>>"$STDERR_LOG" &
PREP_PID=$!
PREP_OUT=$(seq 5 | $OMQ req -b $U -n 5 $T 2>>"$STDERR_LOG" | sort -n | tr '\n' ',')
kill $PREP_PID 2>/dev/null || true; wait $PREP_PID 2>/dev/null || true
check "rep -P2 --echo echoes all" "1,2,3,4,5," "$PREP_OUT"

echo "Parallel GATHER:"
if backend_supports_draft; then
  U=$(ipc_file)
  seq 10 | $OMQ scatter -b $U $T 2>>"$STDERR_LOG" &
  PSCATTER_PID=$!
  wait_for_ipc_bind "$U"
  $OMQ gather -c $U -P 2 $T > $TMPDIR/pgather_out.txt 2>>"$STDERR_LOG" &
  PGATHER_PID=$!
  wait $PGATHER_PID 2>/dev/null || true
  kill $PSCATTER_PID 2>/dev/null || true; wait $PSCATTER_PID 2>/dev/null || true
  PGATHER_CONTENT=$(cat $TMPDIR/pgather_out.txt | sort -n | tr '\n' ',')
  check "gather -P2 receives all messages" "1,2,3,4,5,6,7,8,9,10," "$PGATHER_CONTENT"
else
  skip "draft sockets unsupported by $OMQ_SYSTEM_BACKEND backend"
fi
