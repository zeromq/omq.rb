# frozen_string_literal: true

require_relative "../../test_helper"

describe "eval_send_expr" do
  before do
    @runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "[it.first, *it]"),
      OMQ::PUSH
    )
    @runner.send(:compile_expr)
  end

  it "sets it to message parts" do
    result = @runner.send(:eval_send_expr, ["hello", "world"])
    assert_equal ["hello", "hello", "world"], result
  end

  it "returns nil when expression evaluates to nil" do
    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "nil"),
      OMQ::PUSH
    )
    runner.send(:compile_expr)
    assert_nil runner.send(:eval_send_expr, ["anything"])
  end

  it "returns SENT when expression returns the socket (self <<)" do
    Async do
      OMQ::Transport::Inproc.reset!
      push = OMQ::PUSH.bind("ruby://eval-self-send")
      pull = OMQ::PULL.connect("ruby://eval-self-send")
      runner = OMQ::CLI::PushRunner.new(
        make_config(type_name: "push", send_expr: "self << it"),
        OMQ::PUSH
      )
      runner.send(:compile_expr)
      runner.instance_variable_set(:@sock, push)
      result = runner.send(:eval_send_expr, ["hello"])
      assert_equal OMQ::CLI::BaseRunner::SENT, result
    ensure
      push&.close
      pull&.close
    end
  end

  it "coerces non-string scalar result via #to_s" do
    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "Time.new(2026, 1, 1)"),
      OMQ::PUSH
    )
    runner.send(:compile_expr)
    result = runner.send(:eval_send_expr, nil)
    assert_equal 1, result.size
    assert_instance_of String, result.first
  end

  it "coerces non-string array elements via #to_s" do
    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "[42, :sym, Time.new(2026, 1, 1)]"),
      OMQ::PUSH
    )
    runner.send(:compile_expr)
    result = runner.send(:eval_send_expr, nil)
    assert_equal "42", result[0]
    assert_equal "sym", result[1]
    assert_instance_of String, result[2]
  end

  it "wraps string result in array" do
    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "'hello'"),
      OMQ::PUSH
    )
    runner.send(:compile_expr)
    assert_equal ["hello"], runner.send(:eval_send_expr, nil)
  end
end

describe "eval_recv_expr" do
  it "transforms incoming messages" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", recv_expr: "it.map(&:upcase)"),
      OMQ::PULL
    )
    runner.send(:compile_expr)
    result = runner.send(:eval_recv_expr, ["hello", "world"])
    assert_equal ["HELLO", "WORLD"], result
  end

  it "returns parts unchanged when no recv_expr" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull"),
      OMQ::PULL
    )
    runner.send(:compile_expr)
    result = runner.send(:eval_recv_expr, ["hello"])
    assert_equal ["hello"], result
  end

  it "returns nil when expression evaluates to nil (filtering)" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", recv_expr: "nil"),
      OMQ::PULL
    )
    runner.send(:compile_expr)
    assert_nil runner.send(:eval_recv_expr, ["anything"])
  end

  it "it.first returns the first frame" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", recv_expr: "it.first"),
      OMQ::PULL
    )
    runner.send(:compile_expr)
    result = runner.send(:eval_recv_expr, ["first", "second"])
    assert_equal ["first"], result
  end
end


describe "independent send and recv eval" do
  it "compiles send and recv procs independently" do
    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req", send_expr: "it.map(&:upcase)", recv_expr: "it.map(&:reverse)"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    send_result = runner.send(:eval_send_expr, ["hello"])
    assert_equal ["HELLO"], send_result

    recv_result = runner.send(:eval_recv_expr, ["hello"])
    assert_equal ["olleh"], recv_result
  end

  it "allows send_expr without recv_expr" do
    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req", send_expr: "it.map(&:upcase)"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    send_result = runner.send(:eval_send_expr, ["hello"])
    assert_equal ["HELLO"], send_result

    recv_result = runner.send(:eval_recv_expr, ["hello"])
    assert_equal ["hello"], recv_result
  end

  it "allows recv_expr without send_expr" do
    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req", recv_expr: "it.map(&:upcase)"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    send_result = runner.send(:eval_send_expr, ["hello"])
    assert_equal ["hello"], send_result

    recv_result = runner.send(:eval_recv_expr, ["hello"])
    assert_equal ["HELLO"], recv_result
  end
end


describe "BEGIN/END blocks per direction" do
  it "compiles BEGIN/END for send_expr" do
    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: 'BEGIN{ @count = 0 } @count += 1; it END{ }'),
      OMQ::PUSH
    )
    runner.send(:compile_expr)
    refute_nil runner.instance_variable_get(:@send_begin_proc)
    assert_nil runner.instance_variable_get(:@recv_begin_proc)
  end

  it "compiles BEGIN/END for recv_expr" do
    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull", recv_expr: 'BEGIN{ @sum = 0 } @sum += Integer(it.first); next END{ puts @sum }'),
      OMQ::PULL
    )
    runner.send(:compile_expr)
    refute_nil runner.instance_variable_get(:@recv_begin_proc)
    assert_nil runner.instance_variable_get(:@send_begin_proc)
  end

  it "compiles BEGIN/END independently for both directions" do
    runner = OMQ::CLI::PairRunner.new(
      make_config(type_name: "pair",
                  send_expr: 'BEGIN{ @send_count = 0 } @send_count += 1; it',
                  recv_expr: 'BEGIN{ @recv_count = 0 } @recv_count += 1; it'),
      OMQ::PAIR
    )
    runner.send(:compile_expr)
    refute_nil runner.instance_variable_get(:@send_begin_proc)
    refute_nil runner.instance_variable_get(:@recv_begin_proc)
  end
end

# -- Registration API (OMQ.outgoing / OMQ.incoming) --------------

describe "OMQ.outgoing / OMQ.incoming registration" do
  after do
    # Clean up registered procs between tests
    OMQ.instance_variable_set(:@outgoing_proc, nil)
    OMQ.instance_variable_set(:@incoming_proc, nil)
  end

  it "registers an outgoing proc" do
    OMQ.outgoing { it.map(&:upcase) }
    refute_nil OMQ.outgoing_proc
  end

  it "registers an incoming proc" do
    OMQ.incoming { it.map(&:downcase) }
    refute_nil OMQ.incoming_proc
  end

  it "picks up registered procs during compile_expr" do
    OMQ.outgoing { it.map(&:upcase) }
    OMQ.incoming { it.map(&:reverse) }

    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    refute_nil runner.instance_variable_get(:@send_eval_proc)
    refute_nil runner.instance_variable_get(:@recv_eval_proc)
  end

  it "CLI flags take precedence over registered procs" do
    OMQ.outgoing { raise "should not be called" }

    runner = OMQ::CLI::PushRunner.new(
      make_config(type_name: "push", send_expr: "'cli_wins'"),
      OMQ::PUSH
    )
    runner.send(:compile_expr)

    result = runner.send(:eval_send_expr, ["anything"])
    assert_equal ["cli_wins"], result
  end

  it "uses registered proc when no CLI flag" do
    OMQ.incoming { it.map(&:upcase) }

    runner = OMQ::CLI::PullRunner.new(
      make_config(type_name: "pull"),
      OMQ::PULL
    )
    runner.send(:compile_expr)

    result = runner.send(:eval_recv_expr, ["hello"])
    assert_equal ["HELLO"], result
  end

  it "registered outgoing works without incoming" do
    OMQ.outgoing { it.map(&:upcase) }

    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    refute_nil runner.instance_variable_get(:@send_eval_proc)
    assert_nil runner.instance_variable_get(:@recv_eval_proc)
  end

  it "mixes registered proc on one direction with CLI flag on the other" do
    OMQ.incoming { it.map(&:downcase) }

    runner = OMQ::CLI::ReqRunner.new(
      make_config(type_name: "req", send_expr: "it.map(&:upcase)"),
      OMQ::REQ
    )
    runner.send(:compile_expr)

    send_result = runner.send(:eval_send_expr, ["Hello"])
    assert_equal ["HELLO"], send_result

    recv_result = runner.send(:eval_recv_expr, ["Hello"])
    assert_equal ["hello"], recv_result
  end
end


# -- BEGIN/END blocks -----------------------------------------------

describe "extract_blocks" do
  def ev(src = nil)
    OMQ::CLI::ExpressionEvaluator.new(src, format: :ascii)
  end

  it "extracts BEGIN and END bodies" do
    expr, begin_body, end_body = ev.send(:extract_blocks,
      'BEGIN{ @s = 0 } @s += 1 END{ puts @s }')
    assert_equal " @s = 0 ", begin_body
    assert_equal " puts @s ", end_body
    assert_equal "@s += 1", expr.strip
  end

  it "handles nested braces" do
    expr, begin_body, end_body = ev.send(:extract_blocks,
      'BEGIN{ @h = {} } it END{ @h.each { |k,v| puts k } }')
    assert_equal " @h = {} ", begin_body
    assert_equal " @h.each { |k,v| puts k } ", end_body
    assert_equal "it", expr.strip
  end

  it "returns nil for missing blocks" do
    expr, begin_body, end_body = ev.send(:extract_blocks, 'it')
    assert_nil begin_body
    assert_nil end_body
    assert_equal "it", expr
  end

  it "handles BEGIN only" do
    _, begin_body, end_body = ev.send(:extract_blocks,
      'BEGIN{ @x = 1 } it')
    assert_equal " @x = 1 ", begin_body
    assert_nil end_body
  end

  it "handles END only" do
    _, begin_body, end_body = ev.send(:extract_blocks,
      'it END{ puts "done" }')
    assert_nil begin_body
    assert_equal ' puts "done" ', end_body
  end
end


# -- Explicit block parameters -----------------------------------------

describe "explicit block parameters" do
  def ev(src, format: :ascii)
    OMQ::CLI::ExpressionEvaluator.new(src, format: format)
  end

  it "single param receives whole parts array" do
    e = ev('|msg| msg.map(&:upcase)')
    result = e.call(["hello", "world"], Object.new)
    assert_equal ["HELLO", "WORLD"], result
  end

  it "destructures with parens" do
    e = ev('|(a, b)| [b, a]')
    result = e.call(["hello", "world"], Object.new)
    assert_equal ["world", "hello"], result
  end

  it "works with BEGIN/END blocks" do
    e = ev('BEGIN{ @n = 0 } |msg| @n += 1; msg')
    ctx = Object.new
    ctx.instance_exec(&e.begin_proc)
    result = e.call(["x", "y"], ctx)
    assert_equal ["x", "y"], result
    assert_equal 1, ctx.instance_variable_get(:@n)
  end

  it "without params, it is the parts array" do
    e = ev('it')
    result = e.call(["hello", "world"], Object.new)
    assert_equal ["hello", "world"], result
  end

  it "returns nil for filtering" do
    e = ev('|msg| nil')
    assert_nil e.call(["hello", "world"], Object.new)
  end
end
