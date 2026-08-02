# frozen_string_literal: true

require_relative "../../test_helper"

describe OMQ::CLI::Term do

  describe "format_event_detail" do
    it "returns empty string on nil detail" do
      assert_equal "", OMQ::CLI::Term.format_event_detail(nil)
    end


    it "renders non-hash detail as inspected suffix" do
      assert_equal " foo", OMQ::CLI::Term.format_event_detail("foo")
    end


    it "renders plain EOFError as 'closed by peer'" do
      detail = { error: EOFError.new("Stream finished before reading enough data!"),
                 reason: "Stream finished before reading enough data!" }
      assert_equal " (closed by peer)", OMQ::CLI::Term.format_event_detail(detail)
    end


    it "renders other errors via reason" do
      detail = { error: Errno::ECONNRESET.new("Connection reset by peer"),
                 reason: "Connection reset by peer - Connection reset by peer" }
      assert_equal " (Connection reset by peer - Connection reset by peer)",
                   OMQ::CLI::Term.format_event_detail(detail)
    end


    it "falls back to error.message when reason is missing" do
      detail = { error: Errno::EPIPE.new }
      assert_includes OMQ::CLI::Term.format_event_detail(detail), "Broken pipe"
    end


    it "handles hash detail with only :reason (no :error)" do
      assert_equal " (handshake timeout)",
                   OMQ::CLI::Term.format_event_detail({ reason: "handshake timeout" })
    end


    it "handles empty hash detail" do
      assert_equal "", OMQ::CLI::Term.format_event_detail({})
    end
  end


  describe "format_event" do
    it "formats :disconnected EOFError as 'closed by peer'" do
      event = OMQ::MonitorEvent.new(
        type:     :disconnected,
        endpoint: "tcp://[::1]:5050",
        detail:   { error: EOFError.new("Stream finished before reading enough data!"),
                    reason: "Stream finished before reading enough data!" }
      )
      assert_equal "omq: disconnected tcp://[::1]:5050 (closed by peer)",
                   OMQ::CLI::Term.format_event(event, nil)
    end
  end

end
