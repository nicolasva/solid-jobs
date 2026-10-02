# frozen_string_literal: true

require_relative "test_helper"

class ProcessPayloadJob
  include SolidJobs::Task

  def perform(identifier)
    {
      status: "success",
      job_id: identifier,
      ractor_id: Ractor.current.object_id,
    }
  end
end

class FailingPayloadJob
  include SolidJobs::Task

  def perform
    raise "Application failure"
  end
end

class WorkerTest < Minitest::Test
  def setup
    super
    @redis_config = SolidRedis::Config.new(url: "redis://127.0.0.1:6379/0")
    @payload = Ractor.make_shareable(
      {
        "task" => "ProcessPayloadJob",
        "arguments" => [42],
        "channel" => "default",
        "id" => "0123456789abcdef01234567",
      },
    )
  end

  def test_worker_is_encapsulated_in_an_autonomous_ractor
    result = Ractor.new(@payload, @redis_config) do |payload, redis_config|
      SolidJobs::Executor.new(redis_config: redis_config).execute(payload)
    rescue StandardError => error
      {status: "error", error: error.class.name}
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal "success", result[:status]
    assert_equal 42, result[:job_id]
    refute_equal Ractor.current.object_id, result[:ractor_id]
  end

  def test_shareable_copy_does_not_leak_mutable_payload_state
    mutable = {
      "task" => "ProcessPayloadJob",
      "arguments" => [100],
      "channel" => "default",
    }

    copied = SolidJobs::Utilities.shareable_copy(mutable)
    mutable["arguments"] << 200

    assert Ractor.shareable?(copied)
    assert_equal [100], copied["arguments"]
    assert copied["arguments"].frozen?
  end

  def test_application_error_is_exposed_to_retry_layer
    payload = Ractor.make_shareable(
      {"task" => "FailingPayloadJob", "arguments" => [], "channel" => "critical"},
    )

    result = Ractor.new(payload, @redis_config) do |job, redis_config|
      SolidJobs::Executor.new(redis_config: redis_config).execute(job)
    rescue StandardError => error
      {status: "failed", exception: error.class.name, message: error.message}
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal "failed", result[:status]
    assert_equal "RuntimeError", result[:exception]
    assert_equal "Application failure", result[:message]
  end

  def test_strict_payload_validation
    publisher = SolidJobs::Publisher.new(configuration: @redis_config)

    assert_raises(ArgumentError) { publisher.publish(nil) }
    assert_raises(ArgumentError) { publisher.publish("task" => "", "arguments" => []) }
    assert_raises(ArgumentError) { publisher.publish("task" => "ProcessPayloadJob") }
  end

  def test_channel_routing_uses_canonical_envelope
    publisher = SolidJobs::Publisher.new(configuration: @redis_config)
    task_id = publisher.publish(
      "task" => "ProcessPayloadJob",
      "arguments" => [42],
      "channel" => "critical",
    )
    payload = ProcessPayloadJob.captured.last

    assert_equal task_id, payload["id"]
    assert_equal "ProcessPayloadJob", payload["task"]
    assert_equal "critical", payload["channel"]
  end
end
