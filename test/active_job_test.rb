# frozen_string_literal: true

require_relative "test_helper"

module ActiveJob
  class Base
    def self.execute(data)
      data
    end
  end

  module QueueAdapters
  end
end

require "active_job/queue_adapters/solid_jobs_adapter"

class ExampleActiveJob
  attr_accessor :provider_job_id
  attr_reader :queue_name, :scheduled_at

  def initialize(queue_name: "mailers", scheduled_at: nil)
    @queue_name = queue_name
    @scheduled_at = scheduled_at
  end

  def serialize
    {"job_class" => self.class.name, "arguments" => [42]}
  end
end

class ActiveJobTest < Minitest::Test
  def test_adapter_enqueues_wrapper
    job = ExampleActiveJob.new
    adapter = ActiveJob::QueueAdapters::SolidJobsAdapter.new

    adapter.enqueue(job)
    payload = SolidJobs::ActiveJob::Wrapper.captured.last

    assert_equal job.provider_job_id, payload["id"]
    assert_equal "SolidJobs::ActiveJob::Wrapper", payload["task"]
    assert_equal "ExampleActiveJob", payload["wrapped"]
    assert_equal "mailers", payload["channel"]
  end

  def test_adapter_schedules_wrapper
    job = ExampleActiveJob.new
    adapter = ActiveJob::QueueAdapters::SolidJobsAdapter.new
    timestamp = Time.now.to_f + 60

    adapter.enqueue_at(job, timestamp)

    assert_in_delta timestamp, SolidJobs::ActiveJob::Wrapper.captured.last["run_at"], 0.001
  end
end
