#!/bin/sh
# omq pipe: recv-eval, fan-in, fan-out, HWM reconnect buffering,
# FIFO across source batches, producer-first delivery, compressed pipe.

. "$(dirname "$0")/support.sh"

PIPE_T="$T -l ${OMQ_SYSTEM_LINGER:-60}"
RUST_PIPE_SEND_DELAY=""
[ "$OMQ_SYSTEM_BACKEND" = "rust" ] && RUST_PIPE_SEND_DELAY="-d ${OMQ_SYSTEM_RUST_PIPE_DELAY:-0.5}"

echo "Pipe -e:"
if [ "$OMQ_SYSTEM_BACKEND" = "rust" ]; then
  skip "pipe single-source buffering unsupported by $OMQ_SYSTEM_BACKEND backend"
else
  PIPE_E_IN=$(ipc_file)
  PIPE_E_OUT=$(ipc_file)
  PIPE_E_STDIN="$TMPDIR/pipe_e_stdin"
  mkfifo "$PIPE_E_STDIN"
  exec 3<>"$PIPE_E_STDIN"
  printf '%s\n' "piped" >&3
  $OMQ pull -b "$PIPE_E_OUT" -n 1 $PIPE_T > "$TMPDIR/pipe_e_out.txt" 2>>"$STDERR_LOG" &
  PIPE_E_PULL_PID=$!
  wait_for_ipc_bind "$PIPE_E_OUT"
  $OMQ pipe --in -b "$PIPE_E_IN" --out -c "$PIPE_E_OUT" -e 'it.map(&:upcase)' -n 1 $PIPE_T 2>>"$STDERR_LOG" &
  PIPE_E_PIPE_PID=$!
  wait_for_ipc_bind "$PIPE_E_IN"
  $OMQ push -c "$PIPE_E_IN" $PIPE_T < "$PIPE_E_STDIN" 2>>"$STDERR_LOG" &
  PIPE_E_PUSH_PID=$!
  wait "$PIPE_E_PULL_PID" 2>/dev/null || true
  wait "$PIPE_E_PIPE_PID" 2>/dev/null || true
  exec 3>&-
  kill "$PIPE_E_PUSH_PID" 2>/dev/null || true
  wait "$PIPE_E_PUSH_PID" 2>/dev/null || true
  check "pipe -e transforms in pipeline" "PIPED" "$(cat "$TMPDIR/pipe_e_out.txt")"
fi

echo "Pipe fan-in:"
FANIN_A_URL=$(ipc_file)
FANIN_B_URL=$(ipc_file)
FANIN_OUT_URL=$(ipc_file)
FANIN_A_STDIN="$TMPDIR/fanin_a_stdin"
FANIN_B_STDIN="$TMPDIR/fanin_b_stdin"
mkfifo "$FANIN_A_STDIN" "$FANIN_B_STDIN"
exec 4<>"$FANIN_A_STDIN"
exec 5<>"$FANIN_B_STDIN"
printf '%s\n' "from_a" >&4
printf '%s\n' "from_b" >&5
$OMQ pull -b "$FANIN_OUT_URL" -n 2 $PIPE_T > "$TMPDIR/fanin_out.txt" 2>>"$STDERR_LOG" &
FANIN_PULL_PID=$!
wait_for_ipc_bind "$FANIN_OUT_URL"
$OMQ pipe --in -b "$FANIN_A_URL" -b "$FANIN_B_URL" \
         --out -c "$FANIN_OUT_URL" -e 'it.map(&:upcase)' -n 2 $PIPE_T 2>>"$STDERR_LOG" &
FANIN_PIPE_PID=$!
wait_for_ipc_bind "$FANIN_A_URL"
wait_for_ipc_bind "$FANIN_B_URL"
$OMQ push -c "$FANIN_A_URL" $RUST_PIPE_SEND_DELAY $PIPE_T < "$FANIN_A_STDIN" 2>>"$STDERR_LOG" &
FANIN_A_PID=$!
$OMQ push -c "$FANIN_B_URL" $RUST_PIPE_SEND_DELAY $PIPE_T < "$FANIN_B_STDIN" 2>>"$STDERR_LOG" &
FANIN_B_PID=$!
wait "$FANIN_PULL_PID" 2>/dev/null || true
wait "$FANIN_PIPE_PID" 2>/dev/null || true
exec 4>&-
exec 5>&-
kill "$FANIN_A_PID" "$FANIN_B_PID" 2>/dev/null || true
wait "$FANIN_A_PID" 2>/dev/null || true
wait "$FANIN_B_PID" 2>/dev/null || true
FANIN_LINES=$(wc -l < "$TMPDIR/fanin_out.txt" | tr -d ' ')
FANIN_CONTENT=$(sort "$TMPDIR/fanin_out.txt" | tr '\n' ',')
check "pipe fan-in receives from both sources" "2" "$FANIN_LINES"
check "pipe fan-in content" "FROM_A,FROM_B," "$FANIN_CONTENT"

# Work-stealing (not strict round-robin) distributes messages across
# peers. With only 2 messages, batching may send both to the first
# pump. Send enough messages that both sinks get some.
echo "Pipe fan-out:"
FANOUT_IN_URL=$(ipc_file)
FANOUT_A_URL=$(ipc_file)
FANOUT_B_URL=$(ipc_file)
FANOUT_STDIN="$TMPDIR/fanout_stdin"
mkfifo "$FANOUT_STDIN"
exec 6<>"$FANOUT_STDIN"
seq 20 >&6
$OMQ pull -b "$FANOUT_A_URL" --transient $PIPE_T > "$TMPDIR/fanout_a.txt" 2>>"$STDERR_LOG" &
FANOUT_A_PID=$!
wait_for_ipc_bind "$FANOUT_A_URL"
$OMQ pull -b "$FANOUT_B_URL" --transient $PIPE_T > "$TMPDIR/fanout_b.txt" 2>>"$STDERR_LOG" &
FANOUT_B_PID=$!
wait_for_ipc_bind "$FANOUT_B_URL"
$OMQ pipe --in -b "$FANOUT_IN_URL" \
         --out -c "$FANOUT_A_URL" -c "$FANOUT_B_URL" \
         -e 'it.map(&:upcase)' -n 20 $PIPE_T 2>>"$STDERR_LOG" &
FANOUT_PIPE_PID=$!
wait_for_ipc_bind "$FANOUT_IN_URL"
$OMQ push -c "$FANOUT_IN_URL" $RUST_PIPE_SEND_DELAY $PIPE_T < "$FANOUT_STDIN" 2>>"$STDERR_LOG" &
FANOUT_PUSH_PID=$!
wait "$FANOUT_PIPE_PID" 2>/dev/null || true
wait "$FANOUT_A_PID" 2>/dev/null || true
wait "$FANOUT_B_PID" 2>/dev/null || true
exec 6>&-
kill "$FANOUT_PUSH_PID" 2>/dev/null || true
wait "$FANOUT_PUSH_PID" 2>/dev/null || true
FANOUT_A=$(wc -l < "$TMPDIR/fanout_a.txt" 2>/dev/null | tr -d ' ')
FANOUT_B=$(wc -l < "$TMPDIR/fanout_b.txt" 2>/dev/null | tr -d ' ')
FANOUT_TOTAL=$((FANOUT_A + FANOUT_B))
if [ "$FANOUT_TOTAL" -eq 20 ] && [ "$FANOUT_A" -gt 0 ] && [ "$FANOUT_B" -gt 0 ]; then
  pass "pipe fan-out distributes to both sinks"
else
  fail "pipe fan-out distributes to both sinks" "20 total, both non-empty" "a=$FANOUT_A b=$FANOUT_B total=$FANOUT_TOTAL"
fi

# Use large messages (64KB each) so the kernel buffer fills up and
# creates real backpressure.  With --out --hwm 1, the pipe retains
# un-forwarded messages for a reconnecting consumer.
echo "Pipe send-hwm reconnect:"
if [ "$OMQ_SYSTEM_BACKEND" != "ruby" ]; then
  skip "pipe reconnect buffering unsupported by $OMQ_SYSTEM_BACKEND backend"
else
  PIPE_SRC=$(ipc_file)
  PIPE_DST=$(ipc_file)
  PIPE_STDIN="$TMPDIR/pipe_reconnect_stdin"
  mkfifo "$PIPE_STDIN"
  $OMQ pipe --in -b "$PIPE_SRC" --out -b "$PIPE_DST" --hwm 1 --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  PIPE_PID=$!
  wait_for_ipc_bind "$PIPE_SRC"
  wait_for_ipc_bind "$PIPE_DST"
  $OMQ pull -c "$PIPE_DST" -n 2 $PIPE_T > $TMPDIR/pipe_c1.txt 2>>"$STDERR_LOG" &
  C1_PID=$!
  $OMQ push -c "$PIPE_SRC" $PIPE_T < "$PIPE_STDIN" 2>>"$STDERR_LOG" &
  SRC_PID=$!
  exec 3>"$PIPE_STDIN"
  ruby -e '50.times { |i| puts "#{i}#{"X" * 65536}" }' >&3 &
  PIPE_WRITER_PID=$!
  wait $C1_PID 2>/dev/null || true
  $OMQ pull -c "$PIPE_DST" -n 3 $PIPE_T > $TMPDIR/pipe_c2.txt 2>>"$STDERR_LOG" &
  C2_PID=$!
  if wait $C2_PID 2>/dev/null; then
    C2_LINES=$(wc -l < $TMPDIR/pipe_c2.txt | tr -d ' ')
    check "consumer 2 receives after consumer 1 exits" "3" "$C2_LINES"
  else
    fail "consumer 2 receives after consumer 1 exits" "3 messages" "timeout"
  fi
  exec 3>&-
  wait $PIPE_WRITER_PID 2>/dev/null || true
  kill $PIPE_PID $SRC_PID 2>/dev/null || true
  wait 2>/dev/null || true
fi

echo "Pipe FIFO ordering:"
if [ "$OMQ_SYSTEM_BACKEND" = "rust" ]; then
  skip "pipe FIFO buffering unsupported by $OMQ_SYSTEM_BACKEND backend"
else
  FIFO_SRC=$(ipc_file)
  FIFO_DST=$(ipc_file)
  # Pipe with --out --hwm 1 to create backpressure.
  $OMQ pipe --in -b "$FIFO_SRC" --out -b "$FIFO_DST" --hwm 1 --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  FIFO_PIPE_PID=$!
  wait_for_ipc_bind "$FIFO_SRC"
  wait_for_ipc_bind "$FIFO_DST"

  # Send batch A (messages A0..A9, 64KB each) then batch B (B0..B9).
  ruby -e '10.times { |i| puts "A#{i}#{"X" * 65536}" }' \
    | $OMQ push -c "$FIFO_SRC" $PIPE_T 2>>"$STDERR_LOG"
  ruby -e '10.times { |i| puts "B#{i}#{"Y" * 65536}" }' \
    | $OMQ push -c "$FIFO_SRC" $PIPE_T 2>>"$STDERR_LOG"

  # Consumer pulls 10 messages -- should be A0..A9 in order, no B's mixed in.
  $OMQ pull -c "$FIFO_DST" --hwm 1 -n 10 $PIPE_T > $TMPDIR/fifo_out.txt 2>>"$STDERR_LOG" &
  FIFO_C_PID=$!
  if wait $FIFO_C_PID 2>/dev/null; then
    FIFO_PREFIXES=$(sed 's/[XY].*//' $TMPDIR/fifo_out.txt | tr '\n' ',')
    if [ "$FIFO_PREFIXES" = "A0,A1,A2,A3,A4,A5,A6,A7,A8,A9," ]; then
      pass "pipe preserves FIFO across source batches"
    else
      fail "pipe preserves FIFO across source batches" "A0,A1,...,A9" "$FIFO_PREFIXES"
    fi
  else
    fail "pipe preserves FIFO across source batches" "10 messages" "timeout"
  fi
  kill $FIFO_PIPE_PID 2>/dev/null || true
  wait 2>/dev/null || true
fi

echo "Pipe producer-first:"
if [ "$OMQ_SYSTEM_BACKEND" = "rust" ]; then
  skip "pipe producer-first buffering unsupported by $OMQ_SYSTEM_BACKEND backend"
else
  PF_SRC=$(ipc_file)
  PF_DST=$(ipc_file)
  $OMQ pipe --in -b "$PF_SRC" --out -b "$PF_DST" --hwm 1 --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  PF_PIPE_PID=$!
  wait_for_ipc_bind "$PF_SRC"
  wait_for_ipc_bind "$PF_DST"

  # Producer sends BEFORE consumer exists -- pipe must buffer and deliver.
  seq 5 | $OMQ push -c "$PF_SRC" $PIPE_T 2>>"$STDERR_LOG"

  $OMQ pull -c "$PF_DST" -n 5 $PIPE_T > $TMPDIR/pf_out.txt 2>>"$STDERR_LOG" &
  PF_C_PID=$!
  if wait $PF_C_PID 2>/dev/null; then
    PF_CONTENT=$(cat $TMPDIR/pf_out.txt | tr '\n' ',')
    check "pipe delivers all messages when producer finishes first" "1,2,3,4,5," "$PF_CONTENT"
  else
    fail "pipe delivers all messages when producer finishes first" "5 messages" "timeout"
  fi
  kill $PF_PIPE_PID 2>/dev/null || true
  wait 2>/dev/null || true
fi

echo "Pipe -z (global, both sides):"
if backend_supports_compression; then
  ZC_SRC="ipc://@omq_zc_src_$$"
  ZC_DST="ipc://@omq_zc_dst_$$"
  # -z before --in/--out applies globally to both sockets.
  $OMQ pull -b $ZC_DST -z -n 3 $PIPE_T > $TMPDIR/zc_out.txt 2>>"$STDERR_LOG" &
  ZC_C_PID=$!
  seq 3 | $OMQ push -b $ZC_SRC -z $PIPE_T 2>>"$STDERR_LOG" &
  ZC_SRC_PID=$!
  $OMQ pipe -z --in -c $ZC_SRC --out -c $ZC_DST --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  ZC_PIPE_PID=$!
  if wait $ZC_C_PID 2>/dev/null; then
    ZC_CONTENT=$(cat $TMPDIR/zc_out.txt | tr '\n' ',')
    check "pipe -z end-to-end (global)" "1,2,3," "$ZC_CONTENT"
  else
    fail "pipe -z end-to-end (global)" "3 messages" "timeout"
  fi
  wait $ZC_SRC_PID 2>/dev/null || true
  kill $ZC_PIPE_PID 2>/dev/null || true
  wait 2>/dev/null || true
else
  skip "pipe compression unsupported by $OMQ_SYSTEM_BACKEND backend"
fi

# Per-side compression: only the --in (PULL) side uses zstd+tcp, the
# --out (PUSH) side stays plain tcp. The producer must compress, the
# consumer must NOT.
echo "Pipe --in -z (compress input side only):"
if backend_supports_compression; then
  ZIN_PUB=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
  ZOUT_PUB=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
  $OMQ pull -b tcp://127.0.0.1:$ZOUT_PUB -n 3 $PIPE_T > $TMPDIR/zin_out.txt 2>>"$STDERR_LOG" &
  ZIN_C_PID=$!
  seq 3 | $OMQ push -b tcp://127.0.0.1:$ZIN_PUB -z $PIPE_T 2>>"$STDERR_LOG" &
  ZIN_SRC_PID=$!
  $OMQ pipe --in -z -c tcp://127.0.0.1:$ZIN_PUB --out -c tcp://127.0.0.1:$ZOUT_PUB --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  ZIN_PIPE_PID=$!
  if wait $ZIN_C_PID 2>/dev/null; then
    ZIN_CONTENT=$(cat $TMPDIR/zin_out.txt | tr '\n' ',')
    check "pipe --in -z forwards to uncompressed out" "1,2,3," "$ZIN_CONTENT"
  else
    fail "pipe --in -z forwards to uncompressed out" "3 messages" "timeout"
  fi
  wait $ZIN_SRC_PID 2>/dev/null || true
  kill $ZIN_PIPE_PID 2>/dev/null || true
  wait 2>/dev/null || true
else
  skip "pipe input compression unsupported by $OMQ_SYSTEM_BACKEND backend"
fi

# Mirror: only the --out (PUSH) side uses zstd+tcp.
echo "Pipe --out -z (compress output side only):"
if backend_supports_compression; then
  ZOUT_IN=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
  ZOUT_OUT=$(ruby -e 'require "socket"; s=TCPServer.new("127.0.0.1",0); puts s.addr[1]; s.close')
  $OMQ pull -b tcp://127.0.0.1:$ZOUT_OUT -z -n 3 $PIPE_T > $TMPDIR/zout_out.txt 2>>"$STDERR_LOG" &
  ZOUT_C_PID=$!
  seq 3 | $OMQ push -b tcp://127.0.0.1:$ZOUT_IN $PIPE_T 2>>"$STDERR_LOG" &
  ZOUT_SRC_PID=$!
  $OMQ pipe --in -c tcp://127.0.0.1:$ZOUT_IN --out -z -c tcp://127.0.0.1:$ZOUT_OUT --reconnect-ivl 0.1 $PIPE_T 2>>"$STDERR_LOG" &
  ZOUT_PIPE_PID=$!
  if wait $ZOUT_C_PID 2>/dev/null; then
    ZOUT_CONTENT=$(cat $TMPDIR/zout_out.txt | tr '\n' ',')
    check "pipe --out -z reads uncompressed in, compresses out" "1,2,3," "$ZOUT_CONTENT"
  else
    fail "pipe --out -z reads uncompressed in, compresses out" "3 messages" "timeout"
  fi
  wait $ZOUT_SRC_PID 2>/dev/null || true
  kill $ZOUT_PIPE_PID 2>/dev/null || true
  wait 2>/dev/null || true
else
  skip "pipe output compression unsupported by $OMQ_SYSTEM_BACKEND backend"
fi
