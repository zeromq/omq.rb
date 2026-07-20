# frozen_string_literal: true

module OMQ
  module Transport
    module ZstdTcp
      class Codec
        MAX_DICT_SIZE          = 8 * 1024
        DICT_CAPACITY          = 2 * 1024
        TRAIN_MAX_SAMPLES      = 1000
        TRAIN_MAX_BYTES        = 100 * 1024
        TRAIN_MAX_SAMPLE_LEN   = 2048
        MIN_COMPRESS_NO_DICT   = 512
        MIN_COMPRESS_WITH_DICT = 64

        NUL_PREAMBLE = ("\x00" * 4).b.freeze
        ZSTD_MAGIC   = "\x28\xB5\x2F\xFD".b.freeze
        ZDICT_MAGIC  = "\x37\xA4\x30\xEC".b.freeze

        USER_DICT_ID_RANGE = (32_768..(2**31 - 1)).freeze


        attr_reader :send_dict_bytes, :max_message_size


        def initialize(level:, dict: nil, max_message_size: nil)
          @level            = level
          @max_message_size = max_message_size

          # Start with a no-dict FrameCodec. Once a dict is configured
          # (either via the `dict:` kwarg or via auto-training), this is
          # replaced with a fresh dict-bound FrameCodec. In Zrip, the
          # dict is a permanent property of the codec, so swapping dicts
          # means constructing a new one.
          @send_codec      = Zrip::FrameCodec.new(level: @level)
          @send_dict_bytes = nil

          @training      = dict.nil?
          @train_samples = []
          @train_bytes   = 0

          @cached_parts      = nil
          @cached_compressed = nil

          install_send_dict(dict.b) if dict
        end


        def compress_parts(parts)
          return @cached_compressed if parts.equal?(@cached_parts)

          parts.each { |p| maybe_train!(p) }

          compressed = parts.map { |p| compress_or_plain(p) }
          @cached_parts      = parts
          @cached_compressed = compressed.freeze
          compressed
        end


        def parse_frame_content_size(wire)
          return nil if wire.bytesize < 5

          fhd        = wire.getbyte(4)
          did_flag   = fhd & 0x03
          single_seg = (fhd >> 5) & 0x01
          fcs_flag   = (fhd >> 6) & 0x03

          return nil if fcs_flag == 0 && single_seg == 0

          off = 5 + (single_seg == 0 ? 1 : 0) + [0, 1, 2, 4][did_flag]

          case fcs_flag
          when 0
            return nil if wire.bytesize < off + 1
            wire.getbyte(off)
          when 1
            return nil if wire.bytesize < off + 2
            wire.byteslice(off, 2).unpack1("v") + 256
          when 2
            return nil if wire.bytesize < off + 4
            wire.byteslice(off, 4).unpack1("V")
          when 3
            return nil if wire.bytesize < off + 8
            lo, hi = wire.byteslice(off, 8).unpack("VV")
            (hi << 32) | lo
          end
        end


        private


        def maybe_train!(part)
          return unless @training

          bytes = part.is_a?(String) && part.encoding == Encoding::BINARY ? part : part.to_s.b
          return if bytes.bytesize >= TRAIN_MAX_SAMPLE_LEN

          @train_samples << bytes
          @train_bytes += bytes.bytesize

          return unless @train_samples.size >= TRAIN_MAX_SAMPLES ||
                        @train_bytes >= TRAIN_MAX_BYTES

          trainer = Zrip::DictTrainer.new(DICT_CAPACITY)
          @train_samples.each { |s| trainer.add_sample(s) }
          trained_bytes = trainer.train

          @training = false
          @train_samples = nil

          return if trained_bytes.empty?

          patched = patch_auto_dict_id(trained_bytes)
          install_send_dict(patched)
        end


        def patch_auto_dict_id(bytes)
          out = bytes.dup.b
          id  = rand(USER_DICT_ID_RANGE)
          out[4, 4] = [id].pack("V")
          out
        end


        def install_send_dict(bytes)
          unless bytes.byteslice(0, 4) == ZDICT_MAGIC
            raise ProtocolError, "supplied dict is not ZDICT-format"
          end

          if bytes.bytesize > MAX_DICT_SIZE
            raise ProtocolError, "dict exceeds #{MAX_DICT_SIZE} bytes"
          end

          # Replace the no-dict send codec with a fresh dict-bound one;
          # the old codec is GC'd. Zrip treats dict as a permanent codec
          # property, so install-time is always a fresh build.
          @send_codec      = Zrip::FrameCodec.new(dict: bytes, level: @level)
          @send_dict_bytes = bytes
        end


        def compress_or_plain(part)
          bytes = part.is_a?(String) && part.encoding == Encoding::BINARY ? part : part.to_s.b
          threshold = @send_dict_bytes ? MIN_COMPRESS_WITH_DICT : MIN_COMPRESS_NO_DICT
          return plain(bytes) if bytes.bytesize < threshold

          compressed = @send_codec.compress(bytes)

          return plain(bytes) if compressed.bytesize >= bytes.bytesize - 4

          compressed
        end


        def plain(body)
          NUL_PREAMBLE + body
        end

      end
    end
  end
end
