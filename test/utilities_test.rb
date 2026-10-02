# frozen_string_literal: true

require_relative "test_helper"

class UtilitiesTest < Minitest::Test
  def test_stringifies_nested_keys_without_mutating_the_input
    source = {outer: [{inner: 42}, nil, false], number: 7}

    result = SolidJobs::Utilities.stringify_keys(source)

    assert_equal({"outer" => [{"inner" => 42}, nil, false], "number" => 7}, result)
    assert_equal({outer: [{inner: 42}, nil, false], number: 7}, source)
    refute_same source, result
    refute_same source[:outer], result["outer"]
  end

  def test_preserves_the_last_value_when_stringified_keys_collide
    source = {:key => {first: 1}, "key" => {last: 2}}

    assert_equal({"key" => {"last" => 2}}, SolidJobs::Utilities.stringify_keys(source))
  end

  def test_shallow_conversion_preserves_nested_objects
    nested = {inner: 42}

    result = SolidJobs::Utilities.stringify_hash_keys(outer: nested)

    assert_equal({"outer" => nested}, result)
    assert_same nested, result["outer"]
  end

  def test_scalar_values_are_preserved
    assert_nil SolidJobs::Utilities.stringify_keys(nil)
    [false, true, 7, 1.5, "value", :value].each do |value|
      assert_same value, SolidJobs::Utilities.stringify_keys(value)
    end
  end
end
