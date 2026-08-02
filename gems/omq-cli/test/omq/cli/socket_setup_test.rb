# frozen_string_literal: true

require_relative "../../test_helper"

describe "OMQ::CLI::SocketSetup.compress_url" do
  it "leaves tcp:// unchanged when compression is off" do
    cfg = make_config(type_name: "push")
    assert_equal "tcp://127.0.0.1:5555",
                 OMQ::CLI::SocketSetup.compress_url("tcp://127.0.0.1:5555", cfg)
  end

  it "rewrites tcp:// to zstd+tcp:// for :zstd" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: -3)
    assert_equal "zstd+tcp://127.0.0.1:5555",
                 OMQ::CLI::SocketSetup.compress_url("tcp://127.0.0.1:5555", cfg)
  end

  it "rewrites tcp:// to lz4+tcp:// for :lz4" do
    cfg = make_config(type_name: "push", compress: :lz4)
    assert_equal "lz4+tcp://127.0.0.1:5555",
                 OMQ::CLI::SocketSetup.compress_url("tcp://127.0.0.1:5555", cfg)
  end

  it "leaves non-tcp URLs alone even when compression is on" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: -3)
    assert_equal "ipc://@foo",
                 OMQ::CLI::SocketSetup.compress_url("ipc://@foo", cfg)
  end

  it "picks the per-side codec for :in / :out in a pipe" do
    cfg = make_config(type_name: "pipe",
                      in_compress:  :lz4,
                      out_compress: :zstd, out_compress_level: 3)
    assert_equal "lz4+tcp://x:1",
                 OMQ::CLI::SocketSetup.compress_url("tcp://x:1", cfg, side: :in)
    assert_equal "zstd+tcp://x:2",
                 OMQ::CLI::SocketSetup.compress_url("tcp://x:2", cfg, side: :out)
  end

  it "falls back to global when a side is unset" do
    cfg = make_config(type_name: "pipe",
                      compress: :zstd, compress_level: -3,
                      in_compress: nil, out_compress: nil)
    assert_equal "zstd+tcp://x:1",
                 OMQ::CLI::SocketSetup.compress_url("tcp://x:1", cfg, side: :in)
    assert_equal "zstd+tcp://x:2",
                 OMQ::CLI::SocketSetup.compress_url("tcp://x:2", cfg, side: :out)
  end
end


describe "OMQ::CLI::SocketSetup.compress_opts" do
  it "returns level: for zstd" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: 3)
    assert_equal({ level: 3 }, OMQ::CLI::SocketSetup.compress_opts(cfg))
  end

  it "defaults zstd level when unspecified" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: nil)
    assert_equal({ level: OMQ::CLI::SocketSetup::DEFAULT_ZSTD_LEVEL },
                 OMQ::CLI::SocketSetup.compress_opts(cfg))
  end

  it "returns empty kwargs for lz4" do
    cfg = make_config(type_name: "push", compress: :lz4)
    assert_equal({}, OMQ::CLI::SocketSetup.compress_opts(cfg))
  end

  it "returns empty kwargs when compression is off" do
    cfg = make_config(type_name: "push")
    assert_equal({}, OMQ::CLI::SocketSetup.compress_opts(cfg))
  end
end


describe "OMQ::CLI::SocketSetup.compress_flag" do
  it "labels zstd -3 as -z" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: -3)
    assert_equal "-z", OMQ::CLI::SocketSetup.compress_flag(cfg)
  end

  it "labels zstd 3 as -Z" do
    cfg = make_config(type_name: "push", compress: :zstd, compress_level: 3)
    assert_equal "-Z", OMQ::CLI::SocketSetup.compress_flag(cfg)
  end

  it "labels lz4 as --lz4" do
    cfg = make_config(type_name: "push", compress: :lz4)
    assert_equal "--lz4", OMQ::CLI::SocketSetup.compress_flag(cfg)
  end

  it "returns nil when compression is off" do
    cfg = make_config(type_name: "push")
    assert_nil OMQ::CLI::SocketSetup.compress_flag(cfg)
  end
end


describe "OMQ::CLI::SocketSetup.backend_kwargs" do
  it "uses no constructor kwargs for the default ruby backend" do
    cfg = make_config(type_name: "push")
    assert_equal({}, OMQ::CLI::SocketSetup.backend_kwargs(cfg))
  end

  it "passes through explicit backend names" do
    cfg = make_config(type_name: "push", backend: :rust)
    assert_equal({ backend: :rust }, OMQ::CLI::SocketSetup.backend_kwargs(cfg))
  end

  it "keeps ffi flag compatibility" do
    cfg = make_config(type_name: "push", backend: :ruby, ffi: true)
    assert_equal({ backend: :libzmq }, OMQ::CLI::SocketSetup.backend_kwargs(cfg))
  end

  it "reports effective backend names" do
    assert_equal :ruby, OMQ::CLI::SocketSetup.backend_name(make_config(type_name: "push"))
    assert_equal :rust, OMQ::CLI::SocketSetup.backend_name(make_config(type_name: "push", backend: :rust))
    assert_equal :libzmq, OMQ::CLI::SocketSetup.backend_name(make_config(type_name: "push", backend: :ruby, ffi: true))
  end
end
