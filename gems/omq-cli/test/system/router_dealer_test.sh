#!/bin/sh
# ROUTER/DEALER: dealer identity surfacing on router side, and
# router --target addressing a specific dealer peer by identity.

. "$(dirname "$0")/support.sh"

echo "DEALER/ROUTER:"
U=$(ipc)
$OMQ router -b $U -n 1 $T > $TMPDIR/router_out.txt 2>>"$STDERR_LOG" &
$OMQ dealer -c $U --identity worker-1 -D "hi from dealer" -d 0.3 $T 2>>"$STDERR_LOG"
wait
ROUTER_OUT=$(cat $TMPDIR/router_out.txt)
if echo "$ROUTER_OUT" | grep -q "worker-1" && echo "$ROUTER_OUT" | grep -q "hi from dealer"; then
  pass "router sees dealer identity + message"
else
  fail "router sees dealer identity + message" "worker-1<TAB>hi from dealer" "$ROUTER_OUT"
fi

echo "ROUTER --target:"
U=$(ipc)
$OMQ dealer -c $U --identity "d1" -n 1 $T > $TMPDIR/dealer_recv.txt 2>>"$STDERR_LOG" &
$OMQ router -b $U --target "d1" -D "routed reply" -d 0.3 $T 2>>"$STDERR_LOG" || true
wait
# DEALER receives ["", "routed reply"] -- empty delimiter + payload
check "router --target routes to dealer" "	routed reply" "$(cat $TMPDIR/dealer_recv.txt)"
