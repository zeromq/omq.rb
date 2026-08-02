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

backend_available() {
  backend="$1"
  case "$backend" in
    ruby)
      (cd "$CLI_ROOT" && OMQ_DEV=1 bundle exec ruby -Ilib exe/omq --version >/dev/null 2>&1)
      ;;
    rust|libzmq)
      (cd "$CLI_ROOT" && OMQ_DEV=1 bundle exec ruby -Ilib exe/omq --backend "$backend" --version >/dev/null 2>&1)
      ;;
    *)
      return 1
      ;;
  esac
}

for backend in $BACKENDS; do
  if ! backend_available "$backend"; then
    echo "--- backend: $backend ---"
    echo "SKIP: backend unavailable"
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

if [ -z "$FAILED_FILES" ]; then
  echo "OK - $TOTAL_FILES backend test file(s) passed"
  exit 0
else
  echo "FAIL - failed test files:$FAILED_FILES"
  exit 1
fi
