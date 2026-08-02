#!/bin/sh
# Output formats: JSONL round-trip and quoted format escaping of
# non-printable bytes.

. "$(dirname "$0")/support.sh"

echo "JSONL:"
U=$(ipc)
$OMQ pull -b $U -n 1 -J $T > $TMPDIR/jsonl_out.txt 2>>"$STDERR_LOG" &
echo '["part1","part2"]' | $OMQ push -c $U -J $T 2>>"$STDERR_LOG"
wait
check "jsonl round-trip" '["part1","part2"]' "$(cat $TMPDIR/jsonl_out.txt)"

echo "Quoted format:"
U=$(ipc)
$OMQ pull -b $U -n 1 -Q $T > $TMPDIR/quoted_out.txt 2>>"$STDERR_LOG" &
printf 'hello\001world' | $OMQ push -c $U --raw $T 2>>"$STDERR_LOG"
wait
check "quoted format escapes non-printable" 'hello\x01world' "$(cat $TMPDIR/quoted_out.txt)"
