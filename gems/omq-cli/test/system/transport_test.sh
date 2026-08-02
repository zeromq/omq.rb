#!/bin/sh
# Transport smoke tests: TCP loopback and IPC filesystem socket.

. "$(dirname "$0")/support.sh"

echo "TCP transport:"
$OMQ pull -b tcp://127.0.0.1:17199 -n 1 $T > $TMPDIR/tcp_out.txt 2>>"$STDERR_LOG" &
echo "tcp works" | $OMQ push -c tcp://127.0.0.1:17199 $T 2>>"$STDERR_LOG"
wait
check "tcp transport" "tcp works" "$(cat $TMPDIR/tcp_out.txt)"

echo "IPC filesystem:"
IPC_PATH="$TMPDIR/omq_test.sock"
$OMQ pull -b "ipc://$IPC_PATH" -n 1 $T > $TMPDIR/ipc_fs_out.txt 2>>"$STDERR_LOG" &
echo "ipc works" | $OMQ push -c "ipc://$IPC_PATH" $T 2>>"$STDERR_LOG"
wait
check "ipc filesystem transport" "ipc works" "$(cat $TMPDIR/ipc_fs_out.txt)"
