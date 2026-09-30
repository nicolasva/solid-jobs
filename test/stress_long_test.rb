# frozen_string_literal: true

require_relative "test_helper"
require_relative "stress/stress_test_helper"

class LongStressJob
  include SolidJobs::Job

  def perform(*)
  end
end

class LongStressTest < Minitest::Test
  include SolidJobsStressHelpers

  def test_extended_ractor_enqueue_pressure
    skip "Set SOLID_JOBS_LONG_STRESS=1 to run" unless ENV["SOLID_JOBS_LONG_STRESS"] == "1"

    ractor_count = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_RACTORS", "16"))
    jobs_per_ractor = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_JOBS", "25_000"))
    counts = Array.new(ractor_count) do |ractor_index|
      Ractor.new(ractor_index, jobs_per_ractor) do |index, count|
        SolidJobs.testing!(:fake)
        count.times { |job_index| LongStressJob.perform_async(index, job_index) }
        LongStressJob.jobs.length
      end
    end.map { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal Array.new(ractor_count, jobs_per_ractor), counts
  end

  def test_extended_large_shareable_payload_pressure
    skip "Set SOLID_JOBS_LONG_STRESS=1 to run" unless ENV["SOLID_JOBS_LONG_STRESS"] == "1"

    payload_count = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_PAYLOADS", "5_000"))
    blob_size = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_BLOB_SIZE", "5_000"))
    payloads = Ractor.make_shareable(
      Array.new(payload_count) do |index|
        {
          "class" => "LongStressJob",
          "args" => [index, "A" * blob_size],
          "queue" => "bulk",
        }
      end,
    )

    processed = Ractor.new(payloads) do |jobs|
      jobs.count do |payload|
        payload["args"][0].is_a?(Integer) &&
          payload["args"][1].bytesize == LongStressTest::BLOB_SIZE
      end
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal payload_count, processed
  end

  BLOB_SIZE = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_BLOB_SIZE", "5_000"))
end
