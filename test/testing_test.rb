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
end
