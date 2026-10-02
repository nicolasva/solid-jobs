# frozen_string_literal: true

require_relative "test_helper"
require "solid_jobs/console"

class CLITest < Minitest::Test
  def test_parses_server_options
    options = SolidJobs::Console.new.parse(
      %w[--concurrency 8 --channel critical,3 --channel default --environment production --timeout 15],
    )

    assert_equal 8, options[:concurrency]
    assert_equal [["critical", 3], "default"], options[:channels]
    assert_equal "production", options[:environment]
    assert_equal 15.0, options[:timeout]
  end
end
