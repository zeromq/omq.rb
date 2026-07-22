# frozen_string_literal: true

require_relative "../../test_helper"

describe OMQ::QoS::RetryScheduler do
  it "returns range.begin for the first retry" do
    assert_in_delta 0.1, OMQ::QoS::RetryScheduler.delay(0, 0.1..10.0), 1e-9
  end


  it "doubles the delay for each retry up to the cap" do
    range = 0.1..5.0
    assert_in_delta 0.2, OMQ::QoS::RetryScheduler.delay(1, range), 1e-9
    assert_in_delta 0.4, OMQ::QoS::RetryScheduler.delay(2, range), 1e-9
    assert_in_delta 0.8, OMQ::QoS::RetryScheduler.delay(3, range), 1e-9
  end


  it "caps at range.end" do
    range = 0.1..1.0
    assert_equal 1.0, OMQ::QoS::RetryScheduler.delay(20, range)
  end
end
