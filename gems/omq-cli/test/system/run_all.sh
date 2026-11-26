#!/bin/sh
#
# Runs every test/system/*_test.sh file in sequence and aggregates results.
# Each file is a standalone test suite; this script just chains them.

set -u

SYSTEM_DIR=$(cd "$(dirname "$0")" && pwd)
CLI_ROOT=$(cd "$SYSTEM_DIR/../.." && pwd)
cd "$SYSTEM_DIR"

FAILED_FILES=""
TOTAL_FILES=0
BACKENDS=${OMQ_SYSTEM_BACKENDS:-"ruby rust libzmq"}

echo "=== omq system tests ==="
echo

# Succeeds if omq starts with +backend+; otherwise prints why not.
backend_available() {
  backend="$1"
  case "$backend" in
    ruby)
      flags=""
      ;;
    rust|libzmq)
      flags="--backend $backend"
      ;;
    *)
      echo "unknown backend"
      return 1
      ;;
  esac

  out=$(cd "$CLI_ROOT" && OMQ_DEV=1 bundle exec ruby -Ilib exe/omq $flags --version 2>&1) && return 0
  echo "$out" | grep -v '^[[:space:]]*from ' | head -n 3
  return 1
}

for backend in $BACKENDS; do
  if ! reason=$(backend_available "$backend"); then
    echo "--- backend: $backend ---"

    # The pure Ruby backend must always run; rust and libzmq may be
    # missing on this machine.
    if [ "$backend" = ruby ]; then
      echo "FAIL: backend unavailable"
      FAILED_FILES="$FAILED_FILES ruby/(unavailable)"
    else
      echo "SKIP: backend unavailable"
    fi

    echo "$reason" | sed 's/^/  /'
    echo
    continue
  fi

  for f in *_test.sh; do
    TOTAL_FILES=$((TOTAL_FILES + 1))
    echo "--- backend: $backend / $f ---"
    if OMQ_SYSTEM_BACKEND="$backend" sh "$f"; then
      :
    else
      FAILED_FILES="$FAILED_FILES $backend/$f"
    fi
    echo
  done
done

if [ "$TOTAL_FILES" -eq 0 ]; then
  FAILED_FILES="$FAILED_FILES (no test file ran)"
fi

if [ -z "$FAILED_FILES" ]; then
  echo "OK - $TOTAL_FILES backend test file(s) passed"
  exit 0
else
  echo "FAIL - failed test files:$FAILED_FILES"
  exit 1
fi
