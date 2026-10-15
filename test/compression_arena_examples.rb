# frozen_string_literal: true

# Shared wire-level cases used by both transport specs.
module CompressionArenaExamples
  class WireConnection
    attr_accessor :max_message_size
    attr_reader :sent, :reads

    def initialize(messages = [])
      @messages = messages
      @sent     = []
      @reads    = 0
      @closed   = false
    end

    def receive_message
      @reads += 1
      @messages.shift || raise(EOFError)
    end

    def write_message(parts)
      @sent << parts
    end

    alias send_message write_message

    def write_messages(messages)
      @sent.concat(messages)
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end
  end

  def arena_data(messages)
    messages.map { |parts| Protocol::ZMTP::Codec::Frame.encode_message(parts) }.join.b
  end

  def arena(messages, mode: 0, dict: nil)
    data = arena_data(messages)
    header(mode: mode, size: data.bytesize, count: messages.size) + (mode == 1 ? compress(data, dict: dict) : data)
  end

  def header(mode: 0, reserved: "\0\0\0", size: 2, count: 1)
    sentinel + [mode, reserved, size, count].pack("Ca3VV")
  end

  def reject_wire(wire, max_message_size: nil)
    raw  = WireConnection.new([wire])
    conn = wrap(raw, max_message_size: max_message_size)
    assert_raises(protocol_error) { conn.receive_message }
    assert raw.closed?, "protocol violations must close the connection"
  end

  def self.included(spec)
    spec.class_eval do
      [0, 1].each do |mode|
        it "decodes mode #{mode}, including empty parts and long frames" do
          messages = [["", "a" * 300], ["\xFF\0".b], [""]]
          conn = wrap(WireConnection.new([[arena(messages, mode: mode)]]))
          messages.each { |message| assert_equal message, conn.receive_message }
        end

        it "decodes mode #{mode} with the installed dictionary" do
          messages = [["record: value payload " * 50], ["record: value payload " * 40, ""]]
          raw = WireConnection.new([[dictionary_shipment], [arena(messages, mode: mode, dict: dictionary)]])
          conn = wrap(raw)
          messages.each { |message| assert_equal message, conn.receive_message }
        end
      end

      it "retains unread messages before reading another wire message" do
        raw = WireConnection.new([[arena([["one"], ["two"]])], ["\0\0\0\0three".b], [arena([["four"]])]])
        conn = wrap(raw)
        assert_equal ["one"], conn.receive_message
        assert_equal 1, raw.reads
        assert_equal ["two"], conn.receive_message
        assert_equal 1, raw.reads
        assert_equal 0, conn.last_wire_size_in
        assert_equal ["three"], conn.receive_message
        assert_equal ["four"], conn.receive_message
      end

      it "applies the message budget separately, including part storage" do
        raw = WireConnection.new([[arena(Array.new(256) { ["x"] })]])
        conn = wrap(raw, max_message_size: 65)
        assert_operator raw.max_message_size, :>=, 65_552
        256.times { assert_equal ["x"], conn.receive_message }
        reject_wire([arena([["x"], ["", ""]])], max_message_size: 127)
      end

      it "validates the entire arena before returning its first message" do
        data = arena_data([["valid"]]) + "\x01\x01x".b
        reject_wire([header(size: data.bytesize, count: 2) + data])
      end

      it "rejects invalid headers before invoking the decompressor" do
        [
          header(mode: 2), header(mode: 1, reserved: "\0\1\0"),
          header(mode: 1, size: 0), header(mode: 1, size: 65_537),
          header(mode: 1, count: 0), header(mode: 1, count: 257),
          header(mode: 1) + ("x" * 65_537), sentinel,
        ].each do |wire|
          raw = WireConnection.new([[wire]])
          conn = wrap(raw)
          def conn.decompress_arena(*)
            raise "decoder called on invalid envelope"
          end
          assert_raises(protocol_error) { conn.receive_message }
          assert raw.closed?
        end
      end

      it "rejects command flags, reserved flags and incomplete inner framing" do
        ["\x04\0", "\x08\0", "\0", "\x02\0", "\0\x02x", "\x01\0",
         "\x02" + [1 << 63].pack("Q>")].each do |data|
          reject_wire([header(size: data.bytesize) + data.b])
        end
      end

      it "rejects mismatched lengths, counts and trailing frames" do
        data = arena_data([["a"], ["b"]])
        reject_wire([header(size: data.bytesize + 1, count: 2) + data])
        reject_wire([header(size: data.bytesize, count: 1) + data])
        reject_wire([header(size: data.bytesize, count: 3) + data])
        reject_wire([header(size: data.bytesize + 1, count: 2) + data + "\0"])
      end

      it "rejects arenas in either position of a multipart outer message" do
        wire = arena([["a"]])
        reject_wire([wire, "\0\0\0\0x"])
        reject_wire(["\0\0\0\0x", wire])
      end

      it "accepts the decoded byte limit and rejects excess part counts" do
        message = ["x" * (65_536 - 9)]
        conn = wrap(WireConnection.new([[arena([message])]]))
        assert_equal message, conn.receive_message
        conn = wrap(WireConnection.new([[arena([Array.new(4096, "")])]]))
        assert_equal Array.new(4096, ""), conn.receive_message
        reject_wire([arena([Array.new(4097, "")])])
      end

      it "rejects truncated, trailing and concatenated compressed data" do
        wire = arena([["payload " * 100]], mode: 1)
        reject_wire([wire.byteslice(0...-1)])
        reject_wire([wire + "junk"])
        reject_wire([wire + wire.byteslice(16..)])
        wire[8, 4] = [801].pack("V")
        reject_wire([wire])
      end

      it "consumes one dictionary and rejects repeats and multipart shipments" do
        raw = WireConnection.new([[dictionary_shipment], [dictionary_shipment]])
        conn = wrap(raw)
        assert_raises(protocol_error) { conn.receive_message }
        assert raw.closed?
        reject_wire([dictionary_shipment, "\0\0\0\0x"])
        reject_wire(["\0\0\0\0x", dictionary_shipment])
      end

      it "routes fan-out through compression instead of raw wire writes" do
        conn = wrap(WireConnection.new)
        refute conn.respond_to?(:write_wire)
        refute conn.respond_to?(:write_wire_batch)
      end

      it "disconnects a malformed arena peer without delivering its valid prefix or killing healthy peers" do
        Async do |task|
          task.with_timeout(2) do
            scheme  = sentinel == "LZ4A" ? "lz4+tcp" : "zstd+tcp"
            pull    = OMQ::PULL.new
            bad     = OMQ::PUSH.new
            healthy = OMQ::PUSH.new
            bad.reconnect_interval = 60
            endpoint = pull.bind("#{scheme}://127.0.0.1:0")
            bad.connect("tcp://127.0.0.1:#{endpoint.port}")
            healthy.connect(endpoint.to_s)
            bad.peer_connected.wait
            healthy.peer_connected.wait

            data = arena_data([["must not escape"]]) + "\x01\0".b
            bad << [header(size: data.bytesize, count: 2) + data]
            bad.all_peers_gone.wait
            healthy << "healthy"
            assert_equal ["healthy"], pull.receive
          ensure
            bad&.close
            healthy&.close
            pull&.close
          end
        end.wait
      end
    end
  end
end
