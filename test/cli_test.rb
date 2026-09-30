# frozen_string_literal: true

require_relative "test_helper"
require "solid_jobs/cli"

class CLITest < Minitest::Test
  def test_parses_server_options
    options = SolidJobs::CLI.new.parse(
      %w[--concurrency 8 --queue critical,3 --queue default --environment production --timeout 15],
    )

    assert_equal 8, options[:concurrency]
    assert_equal [["critical", 3], "default"], options[:queues]
    assert_equal "production", options[:environment]
    assert_equal 15.0, options[:timeout]
  end
end
