# frozen_string_literal: true

require_relative "test_helper"

class TestingTest < Minitest::Test
  def test_block_mode_is_restored
    SolidJobs.testing!(:capture)

    SolidJobs.testing!(:execute) do
      assert_equal :execute, SolidJobs::Testing.mode
    end

    assert_equal :capture, SolidJobs::Testing.mode
  end

  def test_storage_is_ractor_local
    HardJob.enqueue("main")

    count = Ractor.new do
      SolidJobs.testing!(:capture)
      HardJob.enqueue("ractor")
      HardJob.captured.length
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal 1, count
    assert_equal [["main"]], HardJob.captured.map { |job| job["arguments"] }
  end

  # Regression: config and testing mode were thread-local, so jobs enqueued
  # from a Puma-style worker thread silently went to a default Redis.
  def test_config_and_mode_are_shared_by_threads_in_the_same_ractor
    main_config = SolidJobs.config

    seen = Thread.new { [SolidJobs.config, SolidJobs::Testing.mode] }.value

    assert_same main_config, seen[0]
    assert_equal :capture, seen[1]
  end

  def test_jobs_enqueued_from_threads_land_in_the_shared_capture_storage
    Array.new(4) { |index| Thread.new { HardJob.enqueue(index) } }.each(&:join)

    assert_equal [0, 1, 2, 3], HardJob.captured.map { |job| job["arguments"].first }.sort
  end
end
