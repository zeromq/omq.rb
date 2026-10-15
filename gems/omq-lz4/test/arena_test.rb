# frozen_string_literal: true

require_relative "test_helper"
require_relative "../../../test/compression_arena_examples"

class OMQLZ4ArenaTest < Minitest::Spec
  include CompressionArenaExamples

  def wrap(raw, max_message_size: nil)
    OMQ::Transport::Lz4Tcp::Lz4Connection.new(raw, send_dict_bytes: nil, max_message_size: max_message_size)
  end

  def sentinel
    "LZ4A".b
  end

  def protocol_error
    OMQ::LZ4::ProtocolError
  end

  def dictionary
    "record: value payload " * 100
  end

  def dictionary_shipment
    "LZ4D" + dictionary
  end

  def compress(data, dict: nil)
    Lz4rip::BlockCodec.new(**(dict ? { dict: dict } : {})).compress(data)
  end
end
