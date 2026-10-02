# frozen_string_literal: true

require_relative "test_helper"

class HardJob
  include SolidJobs::Task
  task_options channel: "critical", max_failures: 5

  attr_reader :performed

  def execute_task(*args)
    self.class.performed << args
  end

  def self.performed
    @performed ||= []
  end
end

class JobTest < Minitest::Test
  def setup
    super
    HardJob.clear_captured
    HardJob.performed.clear
  end

  def test_enqueue_stores_solid_jobs_envelope
    task_id = HardJob.enqueue(1, "two")
    payload = HardJob.captured.fetch(0)

    assert_match(/\A[0-9a-f]{8}-[0-9a-f-]{27}\z/, task_id)
    assert_equal task_id, payload["id"]
    assert_equal "HardJob", payload["task"]
    assert_equal "critical", payload["channel"]
    assert_equal [1, "two"], payload["arguments"]
    assert_kind_of Integer, payload["created_ms"]
  end

  def test_enqueue_after_schedules_job
    before = Time.now.to_f
    HardJob.enqueue_after(60, 1)

    assert_operator HardJob.captured.fetch(0)["run_at"], :>=, before + 59
  end

  def test_execute_executes_job
    SolidJobs.testing!(:execute) { HardJob.enqueue("execute") }

    assert_equal [["execute"]], HardJob.performed
  end

  def test_rejects_non_json_arguments
    error = assert_raises(SolidJobs::InvalidArgumentError) do
      HardJob.enqueue(Time.now)
    end

    assert_match(/JSON-native/, error.message)
  end
end
