#!/bin/sh
# -r <script.rb>: OMQ.incoming/outgoing hooks, at_exit, closure state,
# and CLI -E override semantics.

. "$(dirname "$0")/support.sh"

echo "Script OMQ.incoming:"
cat > $TMPDIR/recv_script.rb <<'RUBY'
OMQ.incoming { |msg| msg.map(&:upcase) }
RUBY
U=$(ipc)
$OMQ pull -b $U -r $TMPDIR/recv_script.rb -n 1 $T > $TMPDIR/script_recv_out.txt 2>>"$STDERR_LOG" &
echo 'hello' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "script OMQ.incoming transforms incoming" "HELLO" "$(cat $TMPDIR/script_recv_out.txt)"

echo "Script OMQ.outgoing:"
cat > $TMPDIR/send_script.rb <<'RUBY'
OMQ.outgoing { it.map(&:upcase) }
RUBY
U=$(ipc)
$OMQ pull -b $U -n 1 $T > $TMPDIR/script_send_out.txt 2>>"$STDERR_LOG" &
echo 'hello' | $OMQ push -c $U -r $TMPDIR/send_script.rb $T 2>>"$STDERR_LOG"
wait
check "script OMQ.outgoing transforms outgoing" "HELLO" "$(cat $TMPDIR/script_send_out.txt)"

echo "Script both directions on REQ:"
cat > $TMPDIR/both_script.rb <<'RUBY'
OMQ.outgoing { |msg| msg.map(&:upcase) }
OMQ.incoming { |first_part, *| first_part.reverse }
RUBY
U=$(ipc)
$OMQ rep -b $U --echo -n 1 $T > /dev/null 2>&1 &
REQ_BOTH_OUT=$(echo 'hello' | $OMQ req -c $U -r $TMPDIR/both_script.rb -n 1 $T 2>>"$STDERR_LOG")
wait
# outgoing upcases "hello" -> "HELLO", rep echoes, incoming reverses -> "OLLEH"
check "script send+recv on REQ" "OLLEH" "$REQ_BOTH_OUT"

echo "Script at_exit:"
cat > $TMPDIR/atexit_script.rb <<RUBY
marker = "$TMPDIR/atexit_marker.txt"
OMQ.incoming { |msg| msg.map(&:upcase) }
at_exit { File.write(marker, "cleanup_ran") }
RUBY
U=$(ipc)
$OMQ pull -b $U -r $TMPDIR/atexit_script.rb -n 1 $T > /dev/null 2>>"$STDERR_LOG" &
echo 'hello' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "script at_exit runs on exit" "cleanup_ran" "$(cat $TMPDIR/atexit_marker.txt 2>/dev/null)"

echo "Script closure state:"
cat > $TMPDIR/closure_script.rb <<'RUBY'
count = 0

OMQ.incoming do
  count += 1
  "msg_#{count}"
end
RUBY
U=$(ipc)
$OMQ pull -b $U -r $TMPDIR/closure_script.rb -n 3 $T > $TMPDIR/closure_out.txt 2>>"$STDERR_LOG" &
printf 'a\nb\nc\n' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "script closure increments across messages" "msg_3" "$(tail -1 $TMPDIR/closure_out.txt)"

echo "Script + CLI override:"
cat > $TMPDIR/override_script.rb <<'RUBY'
OMQ.outgoing { raise "should not be called" }
OMQ.incoming { |msg| msg.map(&:downcase) }
RUBY
U=$(ipc)
$OMQ rep -b $U --echo -n 1 $T > /dev/null 2>&1 &
OVERRIDE_OUT=$(echo 'Hello' | $OMQ req -c $U -r $TMPDIR/override_script.rb -E 'it.map(&:upcase)' -n 1 $T 2>>"$STDERR_LOG")
wait
# CLI -E overrides script outgoing: upcases -> "HELLO", rep echoes, script incoming downcases -> "hello"
check "CLI -E overrides script OMQ.outgoing" "hello" "$OVERRIDE_OUT"

echo "Script at_exit teardown:"
TEARDOWN_LOG="$TMPDIR/teardown_log.txt"
cat > $TMPDIR/teardown_script.rb <<RUBY
log = []
OMQ.incoming { |parts| log << parts.first; parts }
at_exit { File.write("$TEARDOWN_LOG", log.join(",")) }
RUBY
U=$(ipc)
$OMQ pull -b $U -r $TMPDIR/teardown_script.rb -n 3 $T > /dev/null 2>>"$STDERR_LOG" &
printf 'a\nb\nc\n' | $OMQ push -c $U $T 2>>"$STDERR_LOG"
wait
check "at_exit sees accumulated closure state" "a,b,c" "$(cat $TMPDIR/teardown_log.txt 2>/dev/null)"
