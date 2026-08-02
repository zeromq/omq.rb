#!/bin/sh
# PUSH/PULL: basic delivery, multipart framing, empty-line handling,
# IPC abstract namespace, file input, and push -E transform before send.

. "$(dirname "$0")/support.sh"

echo "PUSH/PULL:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/pull_out.txt 2>>"$STDERR_LOG" &
echo task-1 | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "pull receives message" "task-1" "$(cat $TMPDIR/pull_out.txt)"

echo "Multipart:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/multi_out.txt 2>>"$STDERR_LOG" &
printf 'frame1\tframe2\tframe3' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "multipart via tabs" "frame1	frame2	frame3" "$(cat $TMPDIR/multi_out.txt)"

echo "Empty lines:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/empty_out.txt 2>>"$STDERR_LOG" &
printf '\nhello\n' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "empty lines are skipped" "hello" "$(cat $TMPDIR/empty_out.txt)"

echo "IPC abstract namespace:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/abstract_out.txt 2>>"$STDERR_LOG" &
echo 'abstract' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "ipc abstract namespace works" "abstract" "$(cat $TMPDIR/abstract_out.txt)"

echo "Ruby eval on send:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/eval_send_out.txt 2>>"$STDERR_LOG" &
echo 'hello' | $OMQ push -c $U -E 'it.map(&:upcase)' $T 2>>"$STDERR_LOG"
wait
check "push -E transforms before send" "HELLO" "$(cat $TMPDIR/eval_send_out.txt)"

echo "Ruby eval filter:"
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/eval_filter_out.txt 2>>"$STDERR_LOG" &
printf 'skip\nkeep\n' | $OMQ push -c $U -E 'it.first == "skip" ? nil : it' $T 2>>"$STDERR_LOG"
wait
check "push -E nil skips message" "keep" "$(cat $TMPDIR/eval_filter_out.txt)"

echo "File input:"
U=$(ipc)
echo "from file" > $TMPDIR/omq_file_input.txt
$OMQ pull -b $U -n 1 $T > $TMPDIR/file_out.txt 2>>"$STDERR_LOG" &
$OMQ push -c $U -F $TMPDIR/omq_file_input.txt $T 2>>"$STDERR_LOG"
wait
check "-F reads from file" "from file" "$(cat $TMPDIR/file_out.txt)"

# Regression: bind-mode PUSH with piped stdin must wait for a peer
# before draining into HWM. The consumer starts after the push socket
# is bound, so without the peer-wait all 5 messages would go into the
# send queue and be lost at close.
echo "PUSH bind waits for peer (piped stdin):"
U=$(ipc_file)
seq 5 | $OMQ push -b $U $T 2>>"$STDERR_LOG" &
PUSH_PID=$!
wait_for_ipc_bind "$U"
$OMQ pull -c $U -n 5 $T > $TMPDIR/push_bind_wait.txt 2>>"$STDERR_LOG"
wait $PUSH_PID 2>/dev/null
PUSH_BIND_CONTENT=$(tr '\n' ',' < $TMPDIR/push_bind_wait.txt)
check "push -b with piped stdin waits for late pull" "1,2,3,4,5," "$PUSH_BIND_CONTENT"
