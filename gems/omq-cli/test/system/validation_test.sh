#!/bin/sh
# CLI option validation: -e/-E direction mismatch, --target on non-ROUTER,
# pipe --in/--out requirements, duplicate endpoints.

. "$(dirname "$0")/support.sh"

echo "Validation:"
$OMQ push -c tcp://x:1 -e 'it' 2>$TMPDIR/val_err.txt && EXITCODE=0 || EXITCODE=$?
check "-e on send-only socket errors" "1" "$EXITCODE"

$OMQ pull -b tcp://:1 -E 'it' 2>$TMPDIR/val_err2.txt && EXITCODE=0 || EXITCODE=$?
check "-E on recv-only socket errors" "1" "$EXITCODE"

$OMQ router -c tcp://x:1 -E 'it' --target peer1 2>$TMPDIR/val_err3.txt && EXITCODE=0 || EXITCODE=$?
check "-E + --target errors" "1" "$EXITCODE"

echo "Pipe validation:"
$OMQ pipe --in -c tcp://x:1 2>$TMPDIR/val_pipe1.txt && EXITCODE=0 || EXITCODE=$?
check "pipe --in without --out errors" "1" "$EXITCODE"

$OMQ pipe --out -c tcp://x:1 2>$TMPDIR/val_pipe2.txt && EXITCODE=0 || EXITCODE=$?
check "pipe --out without --in errors" "1" "$EXITCODE"

$OMQ req --in -c tcp://x:1 --out -c tcp://x:2 2>$TMPDIR/val_pipe3.txt && EXITCODE=0 || EXITCODE=$?
check "--in/--out on non-pipe errors" "1" "$EXITCODE"

$OMQ pipe -c tcp://x:1 -c tcp://x:1 2>$TMPDIR/val_dup1.txt && EXITCODE=0 || EXITCODE=$?
check "pipe duplicate endpoints errors" "1" "$EXITCODE"

$OMQ push -c tcp://x:1 -b tcp://x:1 2>$TMPDIR/val_dup2.txt && EXITCODE=0 || EXITCODE=$?
check "duplicate endpoints errors" "1" "$EXITCODE"

echo "HWM options:"
U=$(ipc)
$OMQ pull -b $U --hwm 10 -n 1 $T > $TMPDIR/hwm_out.txt 2>>"$STDERR_LOG" &
echo 'hwm test' | $OMQ push -c $U --hwm 10 $T 2>>"$STDERR_LOG"
wait
check "--hwm accepted" "hwm test" "$(cat $TMPDIR/hwm_out.txt)"
