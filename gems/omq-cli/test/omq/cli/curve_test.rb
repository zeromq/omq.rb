# frozen_string_literal: true

require_relative "../../test_helper"
require "nuckle"
require "protocol/zmtp/mechanism/curve"

describe "load_curve_crypto" do
  it "loads rbnacl when explicitly requested" do
    assert_equal "RbNaCl", OMQ::CLI.load_curve_crypto("rbnacl").name
  end

  it "is case-insensitive for rbnacl" do
    assert_equal "RbNaCl", OMQ::CLI.load_curve_crypto("RbNaCl").name
  end

  it "defaults to rbnacl when name is nil" do
    assert_equal "RbNaCl", OMQ::CLI.load_curve_crypto(nil).name
  end

  it "loads nuckle when explicitly requested" do
    assert_equal "Nuckle", OMQ::CLI.load_curve_crypto("nuckle").name
  end

  it "is case-insensitive for nuckle" do
    assert_equal "Nuckle", OMQ::CLI.load_curve_crypto("NUCKLE").name
  end

  it "aborts on unknown backend" do
    assert_raises(SystemExit) { quietly { OMQ::CLI.load_curve_crypto("bogus") } }
  end
end


describe "setup_curve" do
  def make_runner(curve_server: false, curve_server_key: nil, crypto: nil)
    config = make_config(
      type_name:        "rep",
      binds:            ["tcp://localhost:5555"],
      endpoints:        [OMQ::CLI::Endpoint.new("tcp://localhost:5555", true)],
      curve_server:     curve_server,
      curve_server_key: curve_server_key,
      crypto:     crypto,
    )
    runner = OMQ::CLI::BaseRunner.allocate
    runner.instance_variable_set(:@config, config)
    runner.instance_variable_set(:@sock, OMQ::REP.new(nil))
    runner
  end

  it "sets up CURVE server with --crypto nuckle" do
    runner = make_runner(curve_server: true, crypto: "nuckle")
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  end

  it "sets up CURVE server with --crypto rbnacl" do
    runner = make_runner(curve_server: true, crypto: "rbnacl")
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  end

  it "sets up CURVE server with default (rbnacl)" do
    runner = make_runner(curve_server: true)
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  end

  it "sets up CURVE client with server key" do
    key        = Nuckle::PrivateKey.generate
    server_z85 = Protocol::ZMTP::Z85.encode(key.public_key.to_s)

    runner = make_runner(curve_server_key: server_z85, crypto: "nuckle")
    runner.send(:setup_curve)
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  end

  it "picks up OMQ_CRYPTO env var" do
    old = ENV["OMQ_CRYPTO"]
    ENV["OMQ_CRYPTO"] = "nuckle"
    runner = make_runner(curve_server: true)
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  ensure
    if old then ENV["OMQ_CRYPTO"] = old else ENV.delete("OMQ_CRYPTO") end
  end

  it "flag overrides env var" do
    old = ENV["OMQ_CRYPTO"]
    ENV["OMQ_CRYPTO"] = "nuckle"
    runner = make_runner(curve_server: true, crypto: "nuckle")
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  ensure
    if old then ENV["OMQ_CRYPTO"] = old else ENV.delete("OMQ_CRYPTO") end
  end

  it "picks up OMQ_SERVER_PUBLIC and OMQ_SERVER_SECRET env vars" do
    key = Nuckle::PrivateKey.generate
    old_pub = ENV["OMQ_SERVER_PUBLIC"]
    old_sec = ENV["OMQ_SERVER_SECRET"]
    ENV["OMQ_SERVER_PUBLIC"] = Protocol::ZMTP::Z85.encode(key.public_key.to_s)
    ENV["OMQ_SERVER_SECRET"] = Protocol::ZMTP::Z85.encode(key.to_s)

    runner = make_runner(crypto: "nuckle")
    quietly { runner.send(:setup_curve) }
    assert_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  ensure
    if old_pub then ENV["OMQ_SERVER_PUBLIC"] = old_pub else ENV.delete("OMQ_SERVER_PUBLIC") end
    if old_sec then ENV["OMQ_SERVER_SECRET"] = old_sec else ENV.delete("OMQ_SERVER_SECRET") end
  end

  it "does nothing when no CURVE options are set" do
    runner = make_runner
    runner.send(:setup_curve)
    refute_kind_of Protocol::ZMTP::Mechanism::Curve, runner.sock.mechanism
    runner.sock.close rescue nil
  end
end


describe "omq keygen" do
  it "generates Z85 keypair to stdout" do
    out = capture_io { OMQ::CLI.run_keygen(["--crypto", "nuckle"]) }.first
    assert_match(/^OMQ_SERVER_PUBLIC='/, out)
    assert_match(/^OMQ_SERVER_SECRET='/, out)
  end

  it "generates valid 40-char Z85 keys that decode to 32 bytes" do
    out = capture_io { OMQ::CLI.run_keygen(["--crypto", "nuckle"]) }.first
    pub = out[/OMQ_SERVER_PUBLIC='([^']+)'/, 1]
    sec = out[/OMQ_SERVER_SECRET='([^']+)'/, 1]
    assert_equal 40, pub.length
    assert_equal 40, sec.length
    assert_equal 32, Protocol::ZMTP::Z85.decode(pub).bytesize
    assert_equal 32, Protocol::ZMTP::Z85.decode(sec).bytesize
  end

  it "respects --crypto rbnacl" do
    out = capture_io { OMQ::CLI.run_keygen(["--crypto", "rbnacl"]) }.first
    assert_includes out, "OMQ_SERVER_PUBLIC="
  end

  it "respects OMQ_CRYPTO env var" do
    old = ENV["OMQ_CRYPTO"]
    ENV["OMQ_CRYPTO"] = "nuckle"
    out = capture_io { OMQ::CLI.run_keygen([]) }.first
    assert_includes out, "OMQ_SERVER_PUBLIC="
  ensure
    if old then ENV["OMQ_CRYPTO"] = old else ENV.delete("OMQ_CRYPTO") end
  end

  it "prints help with --help" do
    assert_raises(SystemExit) { capture_io { OMQ::CLI.run_keygen(["--help"]) } }
  end

  it "aborts on unknown option" do
    assert_raises(SystemExit) { quietly { OMQ::CLI.run_keygen(["--bogus"]) } }
  end
end
