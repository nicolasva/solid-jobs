# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "timeout"
require "solid_jobs"

module SolidJobsStressHelpers
  # Real-Redis stress tests must not inherit a `:fake` testing mode left in the
  # Ractor by unit tests loaded into the same process.
  def use_real_redis!(config)
    SolidJobs.testing!(:disable)
    SolidJobs.use_config(config)
  end

  def eventually(timeout: 10, interval: 0.01)
    deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + timeout

    loop do
      return true if yield

      if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) >= deadline
        flunk "condition not reached within #{timeout}s"
      end

      sleep interval
    end
  end
end

