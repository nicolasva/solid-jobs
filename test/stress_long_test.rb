# frozen_string_literal: true

require_relative "test_helper"
require_relative "stress/stress_test_helper"

class LongStressJob
  include SolidJobs::Task

  def execute_task(*)
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
        SolidJobs.testing!(:capture)
        count.times { |job_index| LongStressJob.enqueue(index, job_index) }
        LongStressJob.captured.length
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
          "task" => "LongStressJob",
          "arguments" => [index, "A" * blob_size],
          "channel" => "bulk",
        }
      end,
    )

    processed = Ractor.new(payloads) do |jobs|
      jobs.count do |payload|
        payload["arguments"][0].is_a?(Integer) &&
          payload["arguments"][1].bytesize == LongStressTest::BLOB_SIZE
      end
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal payload_count, processed
  end

  BLOB_SIZE = Integer(ENV.fetch("SOLID_JOBS_LONG_STRESS_BLOB_SIZE", "5_000"))
end
