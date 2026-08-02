#!/bin/sh
# REQ/REP patterns: basic send/receive, echo mode, verbose trace order,
# -E generator mode, -E+-e transform split, eval reply modes, and CURVE
# encryption round-trip (skipped when omq-curve isn't installed).

. "$(dirname "$0")/support.sh"

echo "REQ/REP:"
U=$(ipc)
$OMQ rep -b $U -D "PONG" -n 1 $T > $TMPDIR/rep_out.txt 2>>"$STDERR_LOG" &
REQ_OUT=$(echo hello | $OMQ req -c $U -n 1 $T 2>>"$STDERR_LOG")
wait
check "req receives reply" "PONG" "$REQ_OUT"
check "rep receives request" "hello" "$(cat $TMPDIR/rep_out.txt)"

echo "REP echo:"
U=$(ipc)
$OMQ rep -b $U --echo -n 1 $T > /dev/null 2>&1 &
REQ_OUT=$(echo 'echo me' | $OMQ req -c $U -n 1 $T 2>>"$STDERR_LOG")
wait
check "rep --echo echoes back" "echo me" "$REQ_OUT"

# REQ should log >> (send request) then << (recv reply).
# REP should log << (recv request) then >> (send reply).
echo "REQ/REP verbose trace:"
U=$(ipc)
REP_LOG="$TMPDIR/rep_trace.log"
REQ_LOG="$TMPDIR/req_trace.log"
$OMQ rep -b $U -e'it.first.upcase' -n 1 -vvv $T > /dev/null 2>"$REP_LOG" &
echo 'hi' | $OMQ req -c $U -n 1 -vvv $T > /dev/null 2>"$REQ_LOG"
wait

# Strip log prefix ("omq: ") and size annotation ("(NB) ") to get
# "<direction> <payload>" per traced message, in order.
extract_trace() {
  grep -oE 'omq: (>>|<<) \([^)]*\) .*' "$1" \
    | sed -E 's/^omq: (>>|<<) \([^)]*\) /\1 /'
}

REQ_TRACE=$(extract_trace "$REQ_LOG" | tr '\n' '|' | sed 's/|$//')
REP_TRACE=$(extract_trace "$REP_LOG" | tr '\n' '|' | sed 's/|$//')

check "req -vvv trace (>> hi, << HI)" ">> hi|<< HI" "$REQ_TRACE"
check "rep -vvv trace (<< hi, >> HI)" "<< hi|>> HI" "$REP_TRACE"

# REQ with -E and no stdin input should produce requests from the
# eval alone, same as PUSH/PUB generator mode. -n bounds the run.
echo "REQ -E generator:"
U=$(ipc)
$OMQ rep -b $U -e '|(a)| a.upcase' -n 3 $T > $TMPDIR/rep_gen_out.txt 2>>"$STDERR_LOG" &
$OMQ req -c $U -E '"foo"' -n 3 $T > $TMPDIR/req_gen_out.txt 2>>"$STDERR_LOG"
wait
check "req -E generator sends N requests" "FOO
FOO
FOO" "$(cat $TMPDIR/req_gen_out.txt)"
check "rep sends N evaluated replies" "FOO
FOO
FOO" "$(cat $TMPDIR/rep_gen_out.txt)"

echo "Ruby eval:"
U=$(ipc)
$OMQ rep -b $U -e 'it.map(&:upcase)' -n 1 $T > /dev/null 2>&1 &
EVAL_OUT=$(echo 'hello' | $OMQ req -c $U -n 1 $T 2>>"$STDERR_LOG")
wait
check "rep -e upcases reply" "HELLO" "$EVAL_OUT"

echo "Ruby eval nil:"
U=$(ipc)
$OMQ rep -b $U -e 'nil' -n 1 $T > /dev/null 2>&1 &
EVAL_NIL_OUT=$(echo 'anything' | $OMQ req -c $U -n 1 $T 2>>"$STDERR_LOG")
wait
check "rep -e nil sends empty reply" "" "$EVAL_NIL_OUT"

echo "REQ -E and -e:"
U=$(ipc)
$OMQ rep -b $U --echo -n 1 $T > /dev/null 2>&1 &
REQ_SPLIT_OUT=$(echo 'hello' | $OMQ req -c $U -E 'it.map(&:upcase)' -e 'it.map(&:reverse)' -n 1 $T 2>>"$STDERR_LOG")
wait
# -E upcases "hello" -> "HELLO", rep echoes "HELLO", -e reverses -> "OLLEH"
check "req -E sends transformed, -e transforms reply" "OLLEH" "$REQ_SPLIT_OUT"

if backend_supports_curve && ruby -Ilib -e 'require "omq/curve"' 2>>"$STDERR_LOG"; then
  echo "CURVE encryption:"
  U=$(ipc)
  CURVE_KEYS=$(ruby -Ilib -e 'require "omq/curve"; k = RbNaCl::PrivateKey.generate; puts OMQ::Z85.encode(k.public_key.to_s); puts OMQ::Z85.encode(k.to_s)')
  CURVE_PUB=$(echo "$CURVE_KEYS" | head -1)
  CURVE_SEC=$(echo "$CURVE_KEYS" | tail -1)

  OMQ_SERVER_PUBLIC="$CURVE_PUB" OMQ_SERVER_SECRET="$CURVE_SEC" \
    $OMQ rep -b $U -D "secret" -n 1 $T > $TMPDIR/curve_rep_out.txt 2>>"$STDERR_LOG" &

  OMQ_SERVER_KEY="$CURVE_PUB" \
    $OMQ req -c $U -D "classified" -n 1 $T > $TMPDIR/curve_req_out.txt 2>>"$STDERR_LOG"
  wait

  check "curve req receives encrypted reply" "secret" "$(cat $TMPDIR/curve_req_out.txt)"
  check "curve rep receives encrypted request" "classified" "$(cat $TMPDIR/curve_rep_out.txt)"
else
  echo "CURVE: skipped (omq-curve not installed)"
fi
