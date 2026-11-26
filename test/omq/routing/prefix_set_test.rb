# frozen_string_literal: true

require_relative "../../test_helper"

class OMQPrefixSetTest < Minitest::Spec
  PrefixSet = OMQ::Routing::PrefixSet

  describe "PrefixSet" do
    it "matches topics by prefix" do
      set = PrefixSet.new
      set << "weather."
      set << "news"

      assert set.match?("weather.zurich")
      assert set.match?("news")
      refute set.match?("weathe")
      refute set.match?("sports")
      refute set.match?("")
    end


    it "matches everything after an empty subscription" do
      set = PrefixSet.new
      set << ""

      assert set.match?("")
      assert set.match?("anything")

      set.delete("")
      refute set.match?("anything")
    end


    it "stores a prefix once" do
      set = PrefixSet.new

      assert set.add("a")
      refute set.add("a")
      assert_equal 1, set.size
      assert set.include?("a")
      refute set.include?("ab")
    end


    it "splits and merges shared labels" do
      set = PrefixSet.new
      %w[abcd ab abx].each { set << it }

      assert set.match?("abcdef")
      assert set.match?("abz")
      refute set.match?("a")
      refute set.include?("abc"), "an inner node is not a prefix"
      refute set.delete("abc")

      assert set.delete("ab")
      refute set.match?("abz")
      assert set.match?("abxy")
      assert set.match?("abcd")

      assert set.delete("abx")
      assert set.delete("abcd")
      assert set.empty?
      refute set.match?("abcd")
    end


    it "compares bytes regardless of encoding" do
      set = PrefixSet.new
      set << "über".b

      assert set.match?("über.news")
      assert set.include?("über")
      assert set.delete("über")
    end


    it "lists its prefixes" do
      set = PrefixSet.new
      %w[b a ab].each { set << it }

      assert_equal %w[a ab b], set.to_a.sort
      assert set.all? { it.encoding == Encoding::BINARY && it.frozen? }
    end


    it "agrees with a naive model under random changes" do
      rng   = Random.new(1)
      bytes = ["a", "b", "\xC3".b, "\x00".b]
      word  = -> { Array.new(rng.rand(0..5)) { bytes.sample(random: rng) }.join.b }
      set   = PrefixSet.new
      model = Set.new

      1_000.times do
        prefix = word.call

        if rng.rand < 0.6
          assert_equal model.add?(prefix) ? true : false, set.add(prefix)
        else
          assert_equal model.delete?(prefix) ? true : false, set.delete(prefix)
        end

        assert_equal model.size, set.size
        assert_equal model.sort, set.to_a.sort

        topic = word.call + word.call
        assert_equal model.any? { topic.start_with?(it) }, set.match?(topic), "topic #{topic.inspect}"
      end

      model.each { set.delete(it) }
      assert_nil set.instance_variable_get(:@root).children, "no nodes left behind"
    end
  end
end
