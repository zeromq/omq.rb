# frozen_string_literal: true

require_relative "../test_helper"

describe OMQ::QoS do
  describe ".at_least_once" do
    it "builds a level 1 instance with defaults" do
      q = OMQ::QoS.at_least_once
      assert_equal 1, q.level
      assert_equal OMQ::QoS::SUPPORTED_HASH_ALGOS, q.hash_algos
    end


    it "accepts a custom hash_algos list" do
      q = OMQ::QoS.at_least_once(hash_algos: "x")
      assert_equal "x", q.hash_algos
    end
  end


  describe ".exactly_once" do
    it "builds a level 2 instance with defaults" do
      q = OMQ::QoS.exactly_once
      assert_equal 2, q.level
      assert_equal OMQ::QoS::DEFAULT_DEAD_LETTER_TIMEOUT, q.dead_letter_timeout
      assert_equal OMQ::QoS::DEFAULT_DEDUP_TTL, q.dedup_ttl
    end


    it "accepts overrides" do
      q = OMQ::QoS.exactly_once(dead_letter_timeout: 5, dedup_ttl: 10)
      assert_equal 5, q.dead_letter_timeout
      assert_equal 10, q.dedup_ttl
    end
  end


  describe ".exactly_once_and_processed" do
    it "builds a level 3 instance with defaults" do
      q = OMQ::QoS.exactly_once_and_processed
      assert_equal 3, q.level
      assert_equal OMQ::QoS::DEFAULT_MAX_RETRIES, q.max_retries
      assert_nil q.processing_timeout
      assert_equal OMQ::QoS::DEFAULT_RETRY_BACKOFF, q.retry_backoff
    end


    it "accepts overrides" do
      q = OMQ::QoS.exactly_once_and_processed(max_retries: 5, processing_timeout: 1.0)
      assert_equal 5, q.max_retries
      assert_in_delta 1.0, q.processing_timeout
    end
  end


  describe "#attach!" do
    it "accepts re-attach to the same engine (idempotent)" do
      q = OMQ::QoS.at_least_once
      sock = OMQ::PUSH.new
      sock.qos = q
      sock.qos = q
      assert sock.qos.equal?(q)
    ensure
      sock&.close
    end


    it "rejects attaching to a different socket" do
      q = OMQ::QoS.at_least_once
      s1 = OMQ::PUSH.new
      s2 = OMQ::PUSH.new
      s1.qos = q
      err = assert_raises(ArgumentError) { s2.qos = q }
      assert_match(/already attached/, err.message)
    ensure
      s1&.close
      s2&.close
    end
  end
end
