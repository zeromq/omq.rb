# frozen_string_literal: true

require_relative "test_helper"

class OMQBackendRustCompressionArenasTest < Minitest::Spec
  COMPRESSION_AVAILABLE = begin
    require "omq/lz4"
    require "omq/zstd"
    true
  rescue LoadError
    false
  end

  %w[lz4 zstd].each do |compression|
    [[:rust, :ruby], [:ruby, :rust]].each do |sender, receiver|
      [false, true].each do |with_dict|
        it "delivers #{compression} batches from #{sender} to #{receiver}, dictionary=#{with_dict}, with receive HWM 1" do
          skip_without_ruby_backend
          skip "compression gems unavailable" unless COMPRESSION_AVAILABLE

          Async do |task|
            task.with_timeout(3) do
              pull = OMQ::PULL.new(backend: receiver)
              push = OMQ::PUSH.new(backend: sender)
              pull.recv_hwm = 1
              pull.max_message_size = 1024
              endpoint = pull.bind("#{compression}+tcp://127.0.0.1:0").to_s
              options  = with_dict ? { dict: dictionary(compression) } : {}
              push.connect(endpoint, **options)
              push.peer_connected.wait

              messages = 300.times.map { |i| ["message-#{i}", "payload-#{i}" * 30, ""] }
              messages.each { |message| push << message }
              messages.each_with_index do |expected, index|
                assert_equal expected, pull.receive
                task.yield if index % 10 == 0
              end
            ensure
              push&.close
              pull&.close
            end
          end.wait
        end
      end
    end
  end

  def dictionary(compression)
    return "message payload " * 100 if compression == "lz4"

    trainer = Zrip::DictTrainer.new(2048)
    200.times { |i| trainer.add_sample("message-#{i} payload-#{i}" * 5) }
    trainer.train
  end
end
