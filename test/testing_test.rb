# frozen_string_literal: true

require_relative "test_helper"

class TestingTest < Minitest::Test
  def test_block_mode_is_restored
    SolidJobs.testing!(:fake)

    SolidJobs.testing!(:inline) do
      assert_equal :inline, SolidJobs::Testing.mode
    end

    assert_equal :fake, SolidJobs::Testing.mode
  end

  def test_storage_is_ractor_local
    HardJob.perform_async("main")

    count = Ractor.new do
      SolidJobs.testing!(:fake)
      HardJob.perform_async("ractor")
      HardJob.jobs.length
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal 1, count
    assert_equal [["main"]], HardJob.jobs.map { |job| job["args"] }
  end

  # Regression: config and testing mode were thread-local, so jobs enqueued
  # from a Puma-style worker thread silently went to a default Redis.
  def test_config_and_mode_are_shared_by_threads_in_the_same_ractor
    main_config = SolidJobs.config

    seen = Thread.new { [SolidJobs.config, SolidJobs::Testing.mode] }.value

    assert_same main_config, seen[0]
    assert_equal :fake, seen[1]
  end

  def test_jobs_enqueued_from_threads_land_in_the_shared_fake_storage
    Array.new(4) { |index| Thread.new { HardJob.perform_async(index) } }.each(&:join)

    assert_equal [0, 1, 2, 3], HardJob.jobs.map { |job| job["args"].first }.sort
  end
end
