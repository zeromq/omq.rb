# frozen_string_literal: true

require_relative "test_helper"
require_relative "../../../test/compression_arena_examples"

class OMQZstdArenaTest < Minitest::Spec
  include CompressionArenaExamples

  it "closes the connection when a dictionary blob cannot be loaded" do
    reject_wire(["\x37\xA4\x30\xEC".b + "\0" * 4])
  end

  def wrap(raw, max_message_size: nil, send_dict: nil)
    codec = OMQ::Transport::ZstdTcp::Codec.new(level: 1, max_message_size: max_message_size, dict: send_dict)
    OMQ::Transport::ZstdTcp::ZstdConnection.new(raw, codec)
  end

  def sentinel
    "ZSTA".b
  end

  def protocol_error
    OMQ::Transport::ZstdTcp::ProtocolError
  end

  def dictionary
    @dictionary ||= begin
      trainer = Zrip::DictTrainer.new(2048)
      200.times { |i| trainer.add_sample("record: value payload #{i}" * 5) }
      trainer.train
    end
  end

  alias dictionary_shipment dictionary

  def compress(data, dict: nil)
    Zrip::FrameCodec.new(**(dict ? { dict: dict } : {})).compress(data)
  end
end
