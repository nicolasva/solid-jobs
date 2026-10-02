# frozen_string_literal: true

require "minitest/autorun"
require "solid_jobs"

class Minitest::Test
  def setup
    SolidJobs.reset!
    SolidJobs::Testing.clear_all
    SolidJobs.testing!(:capture)
  end
end

