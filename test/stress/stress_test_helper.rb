# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "timeout"
require "solid_jobs"

module SolidJobsStressHelpers
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

