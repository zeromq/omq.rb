#!/bin/sh
# Interval (-i) pacing on both send and recv sides, generator mode via
# -E without stdin, various -E transformations (it/nil/coercion), and
# quantized timing invariants.

. "$(dirname "$0")/support.sh"

echo "Interval:"
U=$(ipc)
$OMQ pull -b $U -n 3 $T > $TMPDIR/interval_out.txt 2>>"$STDERR_LOG" &
$OMQ push -c $U -D "tick" -i 0.1 -n 3 $T 2>>"$STDERR_LOG"
wait
check "interval sends N messages" "3" "$(wc -l < $TMPDIR/interval_out.txt | tr -d ' ')"

echo "Interval with eval:"
U=$(ipc)
$OMQ pull -b $U -n 3 $T > $TMPDIR/interval_eval_out.txt 2>>"$STDERR_LOG" &
$OMQ push -c $U -E '"tick"' -i 0.1 -n 3 $T 2>>"$STDERR_LOG"
wait
check "interval -E generates messages without input" "3" "$(wc -l < $TMPDIR/interval_eval_out.txt | tr -d ' ')"

echo "Eval it:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/eval_line_out.txt 2>>"$STDERR_LOG" &
echo "hello" | $OMQ push -c $U -E 'it.first.upcase' $T 2>>"$STDERR_LOG"
wait
check "-E it.first returns first frame" "HELLO" "$(cat $TMPDIR/eval_line_out.txt)"

echo "Eval nil output:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/eval_nil_out.txt 2>>"$STDERR_LOG" &
printf 'skip\nkeep\n' | $OMQ push -c $U -E 'it.first == "skip" ? nil : it' $T 2>>"$STDERR_LOG"
wait
check "-E nil produces no output" "1" "$(wc -l < $TMPDIR/eval_nil_out.txt | tr -d ' ')"

echo "Eval non-string coercion:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/eval_coerce_out.txt 2>>"$STDERR_LOG" &
echo "x" | $OMQ push -c $U -E '[42, :sym]' $T 2>>"$STDERR_LOG"
wait
check "-E non-string parts coerced via #to_s" "42	sym" "$(cat $TMPDIR/eval_coerce_out.txt)"

echo "Interval timing:"
U=$(ipc)
$OMQ pull -b $U -n 3 $T > /dev/null 2>&1 &
START=$(date +%s%N)
$OMQ push -c $U -D "tick" -i 0.2 -n 3 $T 2>>"$STDERR_LOG"
END=$(date +%s%N)
wait
ELAPSED_MS=$(( (END - START) / 1000000 ))
# 3 messages at 0.2s interval should not complete immediately.
if [ "$ELAPSED_MS" -ge 300 ]; then
  TIMING_OK="yes"
else
  TIMING_OK="no (${ELAPSED_MS}ms)"
fi
check "quantized interval keeps cadence" "yes" "$TIMING_OK"

echo "Interval with send-eval + stdin:"
U=$(ipc)
$OMQ pull -b $U -n 3 $T > $TMPDIR/interval_eval_stdin_out.txt 2>>"$STDERR_LOG" &
seq 3 | $OMQ push -c $U -E 'it << "foo"' -i 0.1 $T 2>>"$STDERR_LOG"
wait
check "interval -E with stdin appends to each line" "3" "$(wc -l < $TMPDIR/interval_eval_stdin_out.txt | tr -d ' ')"
check "interval -E with stdin content" "1	foo" "$(head -1 $TMPDIR/interval_eval_stdin_out.txt)"

echo "Pull with interval:"
U=$(ipc_file)
$OMQ pull -b $U -n 3 -i 0.2 $T > $TMPDIR/pull_interval_out.txt 2>>"$STDERR_LOG" &
PULL_PID=$!
wait_for_ipc_bind "$U"
START=$(date +%s%N)
seq 5 | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait $PULL_PID
END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
check "pull -i receives correct count" "3" "$(wc -l < $TMPDIR/pull_interval_out.txt | tr -d ' ')"
# 3 messages at 0.2s interval should not complete immediately.
if [ "$ELAPSED_MS" -ge 300 ]; then
  PULL_TIMING="yes"
else
  PULL_TIMING="no (${ELAPSED_MS}ms)"
fi
check "pull -i rate-limits recv cadence" "yes" "$PULL_TIMING"
