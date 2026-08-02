#!/bin/sh
# PUB/SUB: topic prefix filtering, PUB -E generator mode, JSONL fan-out,
# and a pub->sub eval pipeline.

. "$(dirname "$0")/support.sh"

PUBSUB_T="$T"

pub_flags() {
  if [ "$OMQ_SYSTEM_BACKEND" = "libzmq" ]; then
    echo "-i 0.2 -n 20 $PUBSUB_T"
  else
    echo "-i 0.05 -n 5 $PUBSUB_T"
  fi
}

wait_for_sub_bind() {
  wait_for_ipc_bind "$1"
}

echo "PUB/SUB:"
U=$(ipc_file)
$OMQ sub -b $U -s "weather." -n 1 $PUBSUB_T > $TMPDIR/sub_out.txt 2>>"$STDERR_LOG" &
SUB_PID=$!
wait_for_sub_bind "$U"
$OMQ pub -c $U -E '"weather.nyc 72F"' $(pub_flags) 2>>"$STDERR_LOG"
wait "$SUB_PID" 2>/dev/null || true
check "sub receives matching message" "weather.nyc 72F" "$(cat $TMPDIR/sub_out.txt)"

# PUB with -E and no stdin input should produce messages from the
# eval alone, same as REQ generator mode. Use -i to keep firing so
# SUB has time to subscribe before messages go out.
echo "PUB -E generator:"
U=$(ipc_file)
$OMQ sub -b $U -s "" -n 3 $PUBSUB_T > $TMPDIR/sub_gen_out.txt 2>>"$STDERR_LOG" &
SUB_PID=$!
wait_for_sub_bind "$U"
$OMQ pub -c $U -E '"tick"' $(pub_flags) 2>>"$STDERR_LOG"
wait "$SUB_PID" 2>/dev/null || true
check "pub -E generator, sub receives N" "tick
tick
tick" "$(cat $TMPDIR/sub_gen_out.txt)"

echo "PUB/SUB eval JSONL:"
U=$(ipc_file)
$OMQ sub -b $U -J -n 1 $PUBSUB_T > $TMPDIR/pubsub_jsonl_out.txt 2>>"$STDERR_LOG" &
SUB_PID=$!
wait_for_sub_bind "$U"
$OMQ pub -c $U -E '%w(foo bar)' $(pub_flags) 2>>"$STDERR_LOG"
wait "$SUB_PID" 2>/dev/null || true
check "pub -E array received as jsonl" '["foo","bar"]' "$(cat $TMPDIR/pubsub_jsonl_out.txt)"

echo "PUB/SUB eval pipe:"
U=$(ipc_file)
$OMQ sub -b $U -e 'it.first' -J -n 1 $PUBSUB_T > $TMPDIR/pubsub_evalpipe_out.txt 2>>"$STDERR_LOG" &
SUB_PID=$!
wait_for_sub_bind "$U"
$OMQ pub -c $U -E '%w(foo bar)' $(pub_flags) 2>>"$STDERR_LOG"
wait "$SUB_PID" 2>/dev/null || true
check "pub -E to sub -e extracts first part" '["foo"]' "$(cat $TMPDIR/pubsub_evalpipe_out.txt)"
