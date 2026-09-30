# frozen_string_literal: true

require_relative "test_helper"

class HardJob
  include SolidJobs::Job
  solid_jobs_options queue: "critical", retry: 5

  attr_reader :performed

  def perform(*args)
    self.class.performed << args
  end

  def self.performed
    @performed ||= []
  end
end

class JobTest < Minitest::Test
  def setup
    super
    HardJob.clear
    HardJob.performed.clear
  end

  def test_perform_async_stores_compatible_payload
    jid = HardJob.perform_async(1, "two")
    payload = HardJob.jobs.fetch(0)

    assert_match(/\A[0-9a-f]{24}\z/, jid)
    assert_equal jid, payload["jid"]
    assert_equal "HardJob", payload["class"]
    assert_equal "critical", payload["queue"]
    assert_equal [1, "two"], payload["args"]
    assert_kind_of Integer, payload["created_at"]
  end

  def test_perform_in_schedules_job
    before = Time.now.to_f
    HardJob.perform_in(60, 1)

    assert_operator HardJob.jobs.fetch(0)["at"], :>=, before + 59
  end

  def test_inline_executes_job
    SolidJobs.testing!(:inline) { HardJob.perform_async("inline") }

    assert_equal [["inline"]], HardJob.performed
  end

  def test_rejects_non_json_arguments
    error = assert_raises(SolidJobs::InvalidArgumentError) do
      HardJob.perform_async(Time.now)
    end

    assert_match(/JSON-native/, error.message)
  end
end

