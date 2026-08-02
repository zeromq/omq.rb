# Shared setup for omq system tests. Sourced by every test/system/*_test.sh
# file. Provides: TMPDIR, OMQ, T, STDERR_LOG, pass/fail/check, ipc().
#
# Each test file runs independently. When TMPDIR and PASS/FAIL are already
# set in the environment (run_all.sh aggregation), we reuse them; otherwise
# we create a fresh TMPDIR and install an EXIT trap that prints the
# per-file summary and cleans up.

set -eu

SYSTEM_DIR=$(cd "$(dirname "$0")" && pwd)
CLI_ROOT=$(cd "$SYSTEM_DIR/../.." && pwd)
cd "$CLI_ROOT"

export OMQ_DEV=1
OMQ_SYSTEM_BACKEND=${OMQ_SYSTEM_BACKEND:-ruby}
case "$OMQ_SYSTEM_BACKEND" in
  ruby)   OMQ_BACKEND_FLAG="" ;;
  rust)   OMQ_BACKEND_FLAG="--backend rust" ;;
  libzmq) OMQ_BACKEND_FLAG="--backend libzmq" ;;
  *)      echo "unknown OMQ_SYSTEM_BACKEND=$OMQ_SYSTEM_BACKEND" >&2; exit 1 ;;
esac

OMQ="bundle exec ruby -Ilib exe/omq $OMQ_BACKEND_FLAG"
T="-t ${OMQ_SYSTEM_TIMEOUT:-60}"

if [ -z "${TMPDIR_SYSTEM:-}" ]; then
  TMPDIR_SYSTEM=$(mktemp -d)
  OWN_TMPDIR=1
else
  OWN_TMPDIR=0
fi
TMPDIR="$TMPDIR_SYSTEM"
export TMPDIR TMPDIR_SYSTEM

PASS=0
FAIL=0

STDERR_LOG="$TMPDIR/stderr.log"
: > "$STDERR_LOG"

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() {
  echo "  FAIL: $1 -- expected: '$2', got: '$3'"
  if [ -s "$STDERR_LOG" ]; then
    echo "        stderr: $(cat "$STDERR_LOG")"
  fi
  FAIL=$((FAIL + 1))
}

check() {
  name="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    pass "$name"
  else
    fail "$name" "$expected" "$actual"
  fi
  : > "$STDERR_LOG"
}

skip() {
  echo "  SKIP: $1"
}

backend_supports_compression() {
  [ "$OMQ_SYSTEM_BACKEND" != "libzmq" ]
}

backend_supports_curve() {
  [ "$OMQ_SYSTEM_BACKEND" != "libzmq" ]
}

backend_supports_draft() {
  [ "$OMQ_SYSTEM_BACKEND" != "libzmq" ]
}

# Unique IPC name per call (abstract namespace, no file cleanup).
# Counter persists across $(ipc) subshells via a file.
IPC_CTR="$TMPDIR/ipc_ctr"
[ -f "$IPC_CTR" ] || echo 0 > "$IPC_CTR"
next_ipc_id() {
  N=$(cat "$IPC_CTR")
  N=$((N + 1))
  echo "$N" > "$IPC_CTR"
  echo "$N"
}

ipc() {
  N=$(next_ipc_id)
  echo "ipc://@omq_test_${$}_${N}"
}

ipc_file() {
  N=$(next_ipc_id)
  echo "ipc://$TMPDIR/omq_test_${$}_${N}.sock"
}

wait_for_ipc_bind() {
  URL="$1"
  TIMEOUT="${2:-${OMQ_SYSTEM_WAIT:-30}}"
  PATHNAME=${URL#ipc://}
  ruby -e '
    path = ARGV[0]
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + ARGV[1].to_f

    until File.socket?(path)
      abort "timeout waiting for #{path}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.01
    end
  ' "$PATHNAME" "$TIMEOUT"
}

system_test_cleanup() {
  rc=$?
  if [ "$FAIL" -eq 0 ] && [ "$rc" -eq 0 ]; then
    [ "$OWN_TMPDIR" = "1" ] && rm -rf "$TMPDIR"
  else
    [ -s "$STDERR_LOG" ] && cat "$STDERR_LOG" >&2
  fi
  echo
  echo "Results: $PASS passed, $FAIL failed"
  if [ "$FAIL" -ne 0 ]; then
    exit 1
  fi
  exit "$rc"
}

trap system_test_cleanup EXIT
