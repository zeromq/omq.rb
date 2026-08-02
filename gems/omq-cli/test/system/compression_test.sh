#!/bin/sh
# Per-frame zstd compression (-z): round-trips over zstd+tcp:// and a
# wire-size trace check that a repeating payload compresses to
# significantly fewer bytes on the wire.

. "$(dirname "$0")/support.sh"

T="-t ${OMQ_SYSTEM_TIMEOUT:-60}"

if ! backend_supports_compression; then
  skip "compression transports unsupported by $OMQ_SYSTEM_BACKEND backend"
  exit 0
fi

# Helper: extract port from "bound to zstd+tcp://host:PORT" in a log file.
# Polls for the concrete bind log line until the normal system wait deadline.
extract_port() {
  _log="$1"
  ruby -e '
    log = ARGV[0]
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + ENV.fetch("OMQ_SYSTEM_WAIT", "30").to_f

    loop do
      if File.file?(log)
        data = File.read(log)
        if data =~ /bound to \S+:(\d+)/
          puts $1
          exit 0
        end
      end

      abort "timeout waiting for bound port in #{log}" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.01
    end
  ' "$_log"
}

# -- Round-trip: large payload ----------------------------------------

echo "Compression (large):"
PAYLOAD=$(ruby -e "puts 'x' * 200")
$OMQ rep -b tcp://127.0.0.1:0 -n 1 --echo -z -v $T > $TMPDIR/compress_out.txt 2>$TMPDIR/compress_rep.log &
REP_PID=$!
PORT=$(extract_port "$TMPDIR/compress_rep.log")
echo "$PAYLOAD" | $OMQ req -c tcp://127.0.0.1:$PORT -n 1 -z $T > $TMPDIR/compress_req_out.txt 2>>"$STDERR_LOG"
wait $REP_PID 2>/dev/null
check "compression round-trip" "$PAYLOAD" "$(cat $TMPDIR/compress_req_out.txt)"

# -- Round-trip: small payload ----------------------------------------

echo "Compression (small):"
$OMQ rep -b tcp://127.0.0.1:0 -n 1 --echo -z -v $T > /dev/null 2>$TMPDIR/compress_small_rep.log &
REP_PID=$!
PORT=$(extract_port "$TMPDIR/compress_small_rep.log")
echo 'tiny' | $OMQ req -c tcp://127.0.0.1:$PORT -n 1 -z $T > $TMPDIR/compress_small_out.txt 2>>"$STDERR_LOG"
wait $REP_PID 2>/dev/null
check "compression round-trip (small)" "tiny" "$(cat $TMPDIR/compress_small_out.txt)"

# -- Wire size trace: 2000 bytes should compress, receiver logs wire= -

echo "Compression wire size trace:"
if [ "$OMQ_SYSTEM_BACKEND" = "rust" ]; then
  skip "Rust backend trace does not report compressed wire size"
else
  PAYLOAD=$(ruby -e "print 'Z' * 2000")
  REP_LOG="$TMPDIR/wire_rep.log"
  $OMQ rep -b tcp://127.0.0.1:0 -n 1 --echo -z -vvv $T > /dev/null 2>"$REP_LOG" &
  REP_PID=$!
  PORT=$(extract_port "$REP_LOG")
  printf '%s' "$PAYLOAD" | $OMQ req -c tcp://127.0.0.1:$PORT -n 1 -z $T > /dev/null 2>>"$STDERR_LOG"
  wait $REP_PID 2>/dev/null

  REP_WIRE=$(grep -oE 'wire=[0-9]+B' "$REP_LOG" | head -1 | grep -oE '[0-9]+' || echo "")

  if [ -n "$REP_WIRE" ] && [ "$REP_WIRE" -lt 2000 ]; then
    pass "rep -vvvz logs wire=${REP_WIRE}B < 2000"
  else
    fail "rep -vvvz wire size" "<2000" "${REP_WIRE:-<missing>}"
    cat "$REP_LOG" >&2
  fi
fi

# -- LZ4 round-trip ---------------------------------------------------

echo "Compression --lz4:"
LZ4_PAYLOAD=$(ruby -e "print 'L' * 500")
$OMQ rep -b tcp://127.0.0.1:0 -n 1 --echo --lz4 -v $T > $TMPDIR/lz4_out.txt 2>$TMPDIR/lz4_rep.log &
LZ4_REP_PID=$!
PORT=$(extract_port "$TMPDIR/lz4_rep.log")
printf '%s' "$LZ4_PAYLOAD" | $OMQ req -c tcp://127.0.0.1:$PORT -n 1 --lz4 $T > $TMPDIR/lz4_req_out.txt 2>>"$STDERR_LOG"
wait $LZ4_REP_PID 2>/dev/null
check "lz4 round-trip" "$LZ4_PAYLOAD" "$(cat $TMPDIR/lz4_req_out.txt)"

# --compress=zstd:N form
echo "--compress=zstd:3:"
ZSPEC_PAYLOAD=$(ruby -e "print 'S' * 300")
$OMQ rep -b tcp://127.0.0.1:0 -n 1 --echo --compress=zstd:3 -v $T > $TMPDIR/zspec_out.txt 2>$TMPDIR/zspec_rep.log &
ZSPEC_REP_PID=$!
PORT=$(extract_port "$TMPDIR/zspec_rep.log")
printf '%s' "$ZSPEC_PAYLOAD" | $OMQ req -c tcp://127.0.0.1:$PORT -n 1 --compress=zstd:3 $T > $TMPDIR/zspec_req_out.txt 2>>"$STDERR_LOG"
wait $ZSPEC_REP_PID 2>/dev/null
check "--compress=zstd:3 round-trip" "$ZSPEC_PAYLOAD" "$(cat $TMPDIR/zspec_req_out.txt)"

# Pipe: --in --lz4 on one socket, --out -z on the other (mixed codecs).
echo "Pipe --in --lz4 + --out -z (mixed codecs):"
MX_IN_PORT=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
MX_OUT_PORT=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
$OMQ pull -b tcp://127.0.0.1:$MX_OUT_PORT -z -n 3 $T > $TMPDIR/mx_out.txt 2>>"$STDERR_LOG" &
MX_C_PID=$!
MX_STDIN="$TMPDIR/mx_stdin"
mkfifo "$MX_STDIN"
$OMQ push -b tcp://127.0.0.1:$MX_IN_PORT --lz4 $T < "$MX_STDIN" 2>>"$STDERR_LOG" &
MX_SRC_PID=$!
$OMQ pipe --in --lz4 -c tcp://127.0.0.1:$MX_IN_PORT --out -z -c tcp://127.0.0.1:$MX_OUT_PORT --reconnect-ivl 0.1 $T 2>>"$STDERR_LOG" &
MX_PIPE_PID=$!
exec 3>"$MX_STDIN"
seq 3 >&3
if wait $MX_C_PID 2>/dev/null; then
  MX_CONTENT=$(cat $TMPDIR/mx_out.txt | tr '\n' ',')
  check "mixed-codec pipe end-to-end" "1,2,3," "$MX_CONTENT"
else
  fail "mixed-codec pipe end-to-end" "3 messages" "timeout"
fi
exec 3>&-
wait $MX_SRC_PID 2>/dev/null || true
kill $MX_PIPE_PID 2>/dev/null || true
wait 2>/dev/null || true
