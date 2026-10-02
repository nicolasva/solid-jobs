# frozen_string_literal: true

require_relative "test_helper"
require_relative "stress/stress_test_helper"

class StressJob
  include SolidJobs::Task

  def perform(*)
  end
end

class StressTest < Minitest::Test
  include SolidJobsStressHelpers

  RACTORS = Integer(ENV.fetch("SOLID_JOBS_STRESS_RACTORS", "4"))
  JOBS_PER_RACTOR = Integer(ENV.fetch("SOLID_JOBS_STRESS_JOBS", "500"))

  def test_parallel_ractors_keep_capture_queues_isolated
    counts = Array.new(RACTORS) do |ractor_index|
      Ractor.new(ractor_index) do |index|
        SolidJobs.testing!(:capture)
        SolidJobs::Testing.clear_all
        StressTest::JOBS_PER_RACTOR.times do |job_index|
          StressJob.enqueue(index, job_index, {"value" => job_index})
        end
        [
          StressJob.captured.length,
          StressJob.captured.first["arguments"],
          StressJob.captured.last["arguments"],
        ]
      end
    end.map { |ractor| SolidJobs::RactorSupport.value(ractor) }

    counts.each_with_index do |(count, first, last), index|
      assert_equal JOBS_PER_RACTOR, count
      assert_equal [index, 0, {"value" => 0}], first
      assert_equal [index, JOBS_PER_RACTOR - 1, {"value" => JOBS_PER_RACTOR - 1}], last
    end
    assert_empty StressJob.captured
  end

  def test_repeated_payload_generation_has_bounded_live_heap_growth
    5.times do
      SolidJobs::Testing.clear_all
      1_000.times { |index| StressJob.enqueue(index) }
    end
    GC.start
    baseline = GC.stat(:heap_live_slots)

    20.times do
      SolidJobs::Testing.clear_all
      1_000.times { |index| StressJob.enqueue(index) }
    end
    SolidJobs::Testing.clear_all
    GC.start

    growth = GC.stat(:heap_live_slots) - baseline
    assert_operator growth, :<, 2_000
  end
end
