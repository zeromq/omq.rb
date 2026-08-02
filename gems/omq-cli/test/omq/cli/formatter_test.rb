# frozen_string_literal: true

require_relative "../../test_helper"

describe OMQ::CLI::Formatter do

  # -- ASCII format -------------------------------------------------

  describe "ascii" do
    before { @fmt = OMQ::CLI::Formatter.new(:ascii) }

    it "encodes single-frame message" do
      assert_equal "hello\n", @fmt.encode(["hello"])
    end

    it "encodes multipart as tab-separated" do
      assert_equal "frame1\tframe2\tframe3\n", @fmt.encode(["frame1", "frame2", "frame3"])
    end

    it "replaces non-printable bytes with dots" do
      assert_equal "hel.o\n", @fmt.encode(["hel\x00o"])
      assert_equal "ab..cd\n", @fmt.encode(["ab\x01\x02cd"])
    end

    it "preserves tabs in output" do
      assert_equal "a\tb\n", @fmt.encode(["a\tb"])
    end

    it "encodes empty message" do
      assert_equal "\n", @fmt.encode([""])
    end

    it "decodes single-frame message" do
      assert_equal ["hello"], @fmt.decode("hello\n")
    end

    it "decodes tab-separated into multipart" do
      assert_equal ["frame1", "frame2"], @fmt.decode("frame1\tframe2\n")
    end

    it "decodes blank line as empty array" do
      assert_equal [], @fmt.decode("\n")
      assert_equal [], @fmt.decode("")
    end

    it "preserves trailing empty frames" do
      assert_equal ["a", ""], @fmt.decode("a\t\n")
      assert_equal ["", "b"], @fmt.decode("\tb\n")
    end

    it "round-trips printable text" do
      parts = ["hello", "world"]
      assert_equal parts, @fmt.decode(@fmt.encode(parts))
    end
  end

  # -- Quoted format ------------------------------------------------

  describe "quoted" do
    before { @fmt = OMQ::CLI::Formatter.new(:quoted) }

    it "encodes printable text unchanged" do
      assert_equal "hello world\n", @fmt.encode(["hello world"])
    end

    it "escapes newlines" do
      assert_equal "line1\\nline2\n", @fmt.encode(["line1\nline2"])
    end

    it "escapes carriage returns" do
      assert_equal "a\\rb\n", @fmt.encode(["a\rb"])
    end

    it "escapes tabs" do
      assert_equal "a\\tb\n", @fmt.encode(["a\tb"])
    end

    it "escapes backslashes" do
      assert_equal "a\\\\b\n", @fmt.encode(["a\\b"])
    end

    it "hex-escapes other non-printable bytes" do
      assert_equal "\\x00\\x01\\x7F\n", @fmt.encode(["\x00\x01\x7f"])
    end

    it "encodes multipart as tab-separated" do
      assert_equal "part1\tpart2\n", @fmt.encode(["part1", "part2"])
    end

    it "decodes escaped newlines" do
      assert_equal ["line1\nline2"], @fmt.decode("line1\\nline2\n")
    end

    it "decodes escaped carriage returns" do
      assert_equal ["a\rb"], @fmt.decode("a\\rb\n")
    end

    it "decodes escaped tabs" do
      assert_equal ["a\tb"], @fmt.decode("a\\tb\n")
    end

    it "decodes escaped backslashes" do
      assert_equal ["a\\b"], @fmt.decode("a\\\\b\n")
    end

    it "decodes hex escapes" do
      assert_equal ["\x00\xff".b], @fmt.decode("\\x00\\xFF\n").map(&:b)
    end

    it "round-trips text with special characters" do
      parts = ["line1\nline2\ttab\\back"]
      assert_equal parts, @fmt.decode(@fmt.encode(parts))
    end

    it "round-trips binary data" do
      binary = (0..255).map(&:chr).join.b
      encoded = @fmt.encode([binary])
      decoded = @fmt.decode(encoded).first.b
      assert_equal binary, decoded
    end

    it "decodes blank line as empty array" do
      assert_equal [], @fmt.decode("\n")
    end
  end

  # -- Raw format ---------------------------------------------------

  describe "raw" do
    before { @fmt = OMQ::CLI::Formatter.new(:raw) }

    it "encodes as ZMTP frames" do
      encoded = @fmt.encode(["hello", "world"])
      assert_equal "\x01\x05hello\x00\x05world".b, encoded
    end

    it "encodes empty message" do
      assert_equal "\x00\x00".b, @fmt.encode([""])
    end

    it "decodes line as single-element array" do
      assert_equal ["hello\n"], @fmt.decode("hello\n")
    end

    it "preserves binary data" do
      binary = "\x00\x01\xff".b
      assert_equal [binary], @fmt.decode(binary)
    end
  end

  # -- JSONL format -------------------------------------------------

  describe "jsonl" do
    before { @fmt = OMQ::CLI::Formatter.new(:jsonl) }

    it "encodes as JSON array" do
      assert_equal "[\"hello\"]\n", @fmt.encode(["hello"])
    end

    it "encodes multipart as JSON array" do
      assert_equal "[\"a\",\"b\",\"c\"]\n", @fmt.encode(["a", "b", "c"])
    end

    it "encodes empty parts" do
      assert_equal "[\"\"]\n", @fmt.encode([""])
    end

    it "decodes JSON array" do
      assert_equal ["hello"], @fmt.decode("[\"hello\"]\n")
    end

    it "decodes multipart JSON array" do
      assert_equal ["a", "b"], @fmt.decode("[\"a\",\"b\"]\n")
    end

    it "round-trips multipart messages" do
      parts = ["frame1", "frame2", "frame3"]
      assert_equal parts, @fmt.decode(@fmt.encode(parts))
    end

    it "handles special JSON characters" do
      parts = ["line\nnew", "tab\there", "quote\"end"]
      assert_equal parts, @fmt.decode(@fmt.encode(parts))
    end
  end

  # -- MessagePack format ------------------------------------------

  describe "msgpack" do
    before { @fmt = OMQ::CLI::Formatter.new(:msgpack) }

    it "encodes as MessagePack" do
      encoded = @fmt.encode(["hello"])
      assert_equal ["hello"], MessagePack.unpack(encoded)
    end

    it "encodes multipart" do
      encoded = @fmt.encode(["a", "b", "c"])
      assert_equal ["a", "b", "c"], MessagePack.unpack(encoded)
    end

    it "decodes from IO stream" do
      data   = MessagePack.pack(["hello", "world"])
      io     = StringIO.new(data)
      result = @fmt.decode_msgpack(io)
      assert_equal ["hello", "world"], result
    end

    it "decodes multiple messages from stream" do
      data = MessagePack.pack(["msg1"]) + MessagePack.pack(["msg2"])
      io   = StringIO.new(data)
      assert_equal ["msg1"], @fmt.decode_msgpack(io)
      assert_equal ["msg2"], @fmt.decode_msgpack(io)
    end

    it "returns nil at EOF" do
      io = StringIO.new("")
      assert_nil @fmt.decode_msgpack(io)
    end
  end

  # -- Preview -----------------------------------------------------

  describe "preview" do
    it "renders a single printable frame" do
      assert_equal "(3B) foo", OMQ::CLI::Formatter.preview(["foo"])
    end

    it "renders an empty frame as empty string marker" do
      assert_equal "(0B) ''", OMQ::CLI::Formatter.preview([""])
    end

    it "renders REP-style envelope with leading empty delimiter (no leading pipe)" do
      # ConnSendPump emits wire-level parts [empty_delimiter, body] for REP.
      assert_equal "(1B 2F) ''|1", OMQ::CLI::Formatter.preview(["", "1"])
    end

    it "joins multiple frames with |" do
      assert_equal "(6B 2F) foo|bar", OMQ::CLI::Formatter.preview(["foo", "bar"])
    end

    it "truncates long printable frames" do
      preview = OMQ::CLI::Formatter.preview(["abcdefghijklmnop"])
      # Single-part messages get a 40-byte preview limit
      assert_equal "(16B) abcdefghijklmnop", preview
    end

    it "truncates single-part frames beyond 40 bytes" do
      preview = OMQ::CLI::Formatter.preview(["a" * 50])
      assert_equal "(50B) #{"a" * 40}…", preview
    end

    it "truncates multipart frames at 12 bytes" do
      preview = OMQ::CLI::Formatter.preview(["abcdefghijklmnop", "x", "y"])
      assert_equal "(18B 3F) abcdefghijkl…|x|y", preview
    end

    it "shows byte length for binary frames" do
      assert_equal "(4B) [4B]", OMQ::CLI::Formatter.preview(["\x00\x01\x02\x03"])
    end

    it "indicates trailing parts when more than 3" do
      preview = OMQ::CLI::Formatter.preview(["a", "b", "c", "d", "e"])
      assert_equal "(5B 5F) a|b|c|…", preview
    end
  end

  # -- Marshal-aware preview ---------------------------------------
  #
  # With -M, a "message" on the wire is a single Marshal-dumped frame
  # wrapping one arbitrary Ruby object. The default preview header
  # ("(1obj)") is useless because the frame count never reflects the
  # actual payload shape. The +format: :marshal+ mode unwraps the
  # outer one-element array and inspects the inner object so the
  # reader sees the real payload.
  describe "preview with format: :marshal" do
    it "inspects an array payload" do
      assert_equal %q{(marshal) [nil, :foo, "bar"]},
        OMQ::CLI::Formatter.preview([nil, :foo, "bar"], format: :marshal)
    end

    it "shows uncompressed size when provided" do
      assert_equal %q{(135B marshal) [nil, :foo, "bar"]},
        OMQ::CLI::Formatter.preview([nil, :foo, "bar"], format: :marshal, uncompressed_size: 135)
    end

    it "shows wire_size alongside uncompressed size" do
      assert_equal %q{(135B wire=50B marshal) [nil, :foo, "bar"]},
        OMQ::CLI::Formatter.preview([nil, :foo, "bar"],
          format: :marshal, uncompressed_size: 135, wire_size: 50)
    end

    it "omits wire_size when uncompressed_size is missing" do
      assert_equal %q{(marshal) [nil, :foo, "bar"]},
        OMQ::CLI::Formatter.preview([nil, :foo, "bar"], format: :marshal, wire_size: 50)
    end

    it "inspects scalar payloads" do
      assert_equal "(marshal) 42",
        OMQ::CLI::Formatter.preview(42, format: :marshal)
      assert_equal "(marshal) :hello",
        OMQ::CLI::Formatter.preview(:hello, format: :marshal)
      assert_equal "(marshal) nil",
        OMQ::CLI::Formatter.preview(nil, format: :marshal)
    end

    it "inspects string payloads (no array wrapping)" do
      assert_equal '(marshal) "foo"',
        OMQ::CLI::Formatter.preview("foo", format: :marshal)
    end

    it "inspects hash payloads" do
      assert_equal '(marshal) {a: 1, b: 2}',
        OMQ::CLI::Formatter.preview({ a: 1, b: 2 }, format: :marshal)
    end

    it "truncates long inspected payloads" do
      preview = OMQ::CLI::Formatter.preview("x" * 200, format: :marshal)
      assert_equal %Q{(marshal) "#{"x" * 59}…}.b, preview.b
    end

    it "sanitizes newlines/tabs in the inspected form" do
      # Shouldn't actually appear — String#inspect already escapes —
      # but defense-in-depth: a custom #inspect could leak raw control
      # chars and break the single-line guarantee.
      obj = Object.new
      def obj.inspect = "line1\nline2\tend"
      preview = OMQ::CLI::Formatter.preview(obj, format: :marshal)
      refute_includes preview, "\n"
      refute_includes preview, "\t"
      assert_match(/line1\\nline2\\tend/, preview)
    end
  end

  describe "encode with :marshal" do
    it "inspects the raw Ruby object (not an array of frames)" do
      fmt = OMQ::CLI::Formatter.new(:marshal)
      assert_equal %Q{"foo"\n},       fmt.encode("foo")
      assert_equal %Q{[1, 2, 3]\n},   fmt.encode([1, 2, 3])
      assert_equal "{\"foo\" => #<Encoding:UTF-8>}\n",
        fmt.encode({ "foo" => Encoding::UTF_8 })
    end
  end
end
