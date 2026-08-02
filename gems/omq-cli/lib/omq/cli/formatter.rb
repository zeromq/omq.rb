# frozen_string_literal: true

module OMQ
  module CLI
    # Handles encoding/decoding messages in the configured format.
    #
    class Formatter
      # @param format [Symbol] wire format (:ascii, :quoted, :raw, :jsonl, :msgpack, :marshal)
      def initialize(format)
        @format = format
      end


      # Encodes message parts into a printable string for output.
      #
      # @param parts [Array<String>] message frames
      # @return [String] formatted output line
      def encode(parts)
        case @format
        when :ascii
          parts.map { |p| p.b.gsub(/[^[:print:]\t]/, ".") }.join("\t") << "\n"
        when :quoted
          parts.map { |p| p.b.dump[1..-2] }.join("\t") << "\n"
        when :raw
          parts.each_with_index.map do |p, i|
            Protocol::ZMTP::Codec::Frame.new(p.to_s, more: i < parts.size - 1).to_wire
          end.join
        when :jsonl
          JSON.generate(parts) << "\n"
        when :msgpack
          MessagePack.pack(parts)
        when :marshal
          # Under -M, `parts` is a single Ruby object (not a frame array).
          parts.inspect << "\n"
        end
      end


      # Decodes a formatted input line into message parts.
      #
      # @param line [String] input line (newline-terminated)
      # @return [Array<String>] message frames
      def decode(line)
        case @format
        when :ascii, :marshal
          line.chomp.split("\t", -1)
        when :quoted
          line.chomp.split("\t", -1).map { |p| "\"#{p}\"".undump }
        when :raw
          [line]
        when :jsonl
          arr = JSON.parse(line.chomp)
          abort "JSON Lines input must be an array of strings" unless arr.is_a?(Array) && arr.all? { |e| e.is_a?(String) }
          arr
        end
      end


      # Decodes one Marshal object from the given IO stream.
      #
      # @param io [IO] input stream
      # @return [Object, nil] deserialized object, or nil on EOF
      def decode_marshal(io)
        Marshal.load(io)
      rescue EOFError, TypeError
        nil
      end


      # Decodes one MessagePack object from the given IO stream.
      #
      # @param io [IO] input stream
      # @return [Object, nil] deserialized object, or nil on EOF
      def decode_msgpack(io)
        @msgpack_unpacker ||= MessagePack::Unpacker.new(io)
        @msgpack_unpacker.read
      rescue EOFError
        nil
      end


      # Whitespace/backslash → visible escape sequence used by
      # {Formatter.sanitize}. Everything else outside printable ASCII
      # collapses to '.' via a single String#tr call.
      LINE_ESCAPES = {
        "\t" => '\\t',
        "\n" => '\\n',
        "\r" => '\\r',
        "\\" => '\\\\',
      }.freeze


      # Formats message parts for human-readable preview (logging).
      # When +wire_size+ is given (`omq-zstd`), the header
      # also shows the compressed on-the-wire size: "(29B wire=12B)".
      # Accepts either wire-side Array<String> (monitor events) or
      # post-decode app parts that may contain non-String objects
      # (e.g. -M Marshal.load output).
      #
      # When +format+ is +:marshal+, +parts+ is the raw Ruby object
      # itself (not an Array of frames); the preview inspects it so
      # the reader sees the actual payload structure (e.g.
      # `[nil, :foo, "bar"]`) instead of a meaningless "1obj" header.
      # For marshal, +uncompressed_size+ is the Marshal.dump bytesize
      # (known to the caller, which already serialized for send or
      # received the wire frame for recv) — passed through instead of
      # redumping here.
      #
      # @param parts [Array<String, Object>, Object] message frames, or raw object when +format+ is :marshal
      # @param format [Symbol, nil] active CLI format (:marshal enables object-inspect mode)
      # @param wire_size [Integer, nil] compressed bytes on the wire
      # @param uncompressed_size [Integer, nil] plaintext bytes (marshal only)
      # @return [String] truncated preview of each frame joined by |
      def self.preview(parts, format: nil, wire_size: nil, uncompressed_size: nil)
        case format
        when :marshal
          marshal_preview(parts, uncompressed_size: uncompressed_size, wire_size: wire_size)
        else
          frames_preview(parts, format: format, wire_size: wire_size)
        end
      end


      def self.marshal_preview(parts, uncompressed_size:, wire_size:)
        inspected = parts.inspect
        truncated = inspected.bytesize > 60
        inspected = inspected.byteslice(0, 60) if truncated
        body      = sanitize(inspected)

        body << "…" if truncated

        header = case
                 when uncompressed_size && wire_size
                   "(#{uncompressed_size}B wire=#{wire_size}B marshal)"
                 when uncompressed_size
                   "(#{uncompressed_size}B marshal)"
                 else
                   "(marshal)"
                 end

        "#{header} #{body}"
      end
      private_class_method :marshal_preview


      def self.frames_preview(parts, format:, wire_size:)
        nparts = parts.size
        # Budget per frame: fewer parts → more room for each preview.
        limit  = nparts <= 2 ? 40 : 12
        shown  = parts.first(3).map { |p| preview_frame(p, limit: limit) }
        tail   = nparts > 3 ? "|…" : ""
        total  = parts.all?(String) ? parts.sum { |p| p.bytesize } : nil
        size   = if wire_size && total
                   "#{total}B wire=#{wire_size}B"
                 elsif total
                   "#{total}B"
                 else
                   "#{nparts}obj"
                 end
        header = nparts > 1 ? "(#{size} #{nparts}F)" : "(#{size})"

        "#{header} #{shown.join("|")}#{tail}"
      end
      private_class_method :frames_preview


      # Renders one frame or decoded object for {Formatter.preview}.
      # Strings are sanitized byte-wise (first +limit+ bytes); non-String
      # objects fall back to #inspect (always single-line) truncated
      # at +limit+ bytes.
      #
      # @param part [String, Object]
      # @param limit [Integer] max bytes to show (default 12)
      # @return [String]
      def self.preview_frame(part, limit: 12)
        unless part.is_a?(String)
          s = part.inspect
          return s.bytesize > limit ? "#{s.byteslice(0, limit)}…" : s
        end

        bytes = part.b
        # Empty frames must render as a visible marker, not as the empty
        # string — otherwise joining with "|" would produce misleading
        # output like "|body" for REP/REQ-style envelopes where the first
        # wire frame is an empty delimiter.
        return "''" if bytes.empty?

        sample    = bytes[0, limit]
        printable = sample.count("\x20-\x7e")

        if printable < sample.bytesize / 2
          "[#{bytes.bytesize}B]"
        elsif bytes.bytesize > limit
          "#{sanitize(sample)}…"
        else
          sanitize(sample)
        end
      end


      # Escapes bytes so a preview/body line is guaranteed single-line
      # on a shared tty. Tab/newline/CR/backslash render as literal
      # \t/\n/\r/\\; other non-printables collapse to '.'. Forced to
      # binary encoding first to prevent UTF-8 quirks from rendering
      # raw LF bytes.
      #
      # @param bytes [String]
      # @return [String]
      def self.sanitize(bytes)
        bytes.b.gsub(/[\t\n\r\\]/, LINE_ESCAPES).tr("^ -~", ".")
      end
    end
  end
end
