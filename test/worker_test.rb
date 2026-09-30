# frozen_string_literal: true

require_relative "test_helper"

class ProcessPayloadJob
  include SolidJobs::Job

  def perform(identifier)
    {
      status: "success",
      job_id: identifier,
      ractor_id: Ractor.current.object_id,
    }
  end
end

class FailingPayloadJob
  include SolidJobs::Job

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
        "class" => "ProcessPayloadJob",
        "args" => [42],
        "queue" => "default",
        "jid" => "0123456789abcdef01234567",
      },
    )
  end

  def test_worker_is_encapsulated_in_an_autonomous_ractor
    result = Ractor.new(@payload, @redis_config) do |payload, redis_config|
      SolidJobs::Worker.new(redis_config: redis_config).perform(payload)
    rescue StandardError => error
      {status: "error", error: error.class.name}
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal "success", result[:status]
    assert_equal 42, result[:job_id]
    refute_equal Ractor.current.object_id, result[:ractor_id]
  end

  def test_shareable_copy_does_not_leak_mutable_payload_state
    mutable = {
      "class" => "ProcessPayloadJob",
      "args" => [100],
      "queue" => "default",
    }

    copied = SolidJobs::Utilities.shareable_copy(mutable)
    mutable["args"] << 200

    assert Ractor.shareable?(copied)
    assert_equal [100], copied["args"]
    assert copied["args"].frozen?
  end

  def test_application_error_is_exposed_to_retry_layer
    payload = Ractor.make_shareable(
      {"class" => "FailingPayloadJob", "args" => [], "queue" => "critical"},
    )

    result = Ractor.new(payload, @redis_config) do |job, redis_config|
      SolidJobs::Worker.new(redis_config: redis_config).perform(job)
    rescue StandardError => error
      {status: "failed", exception: error.class.name, message: error.message}
    end.then { |ractor| SolidJobs::RactorSupport.value(ractor) }

    assert_equal "failed", result[:status]
    assert_equal "RuntimeError", result[:exception]
    assert_equal "Application failure", result[:message]
  end

  def test_strict_payload_validation
    client = SolidJobs::Client.new(configuration: @redis_config)

    assert_raises(ArgumentError) { client.push(nil) }
    assert_raises(ArgumentError) { client.push("job_class" => "", "args" => []) }
    assert_raises(ArgumentError) { client.push("job_class" => "ProcessPayloadJob") }
  end

  def test_queue_routing_uses_canonical_payload
    client = SolidJobs::Client.new(configuration: @redis_config)
    jid = client.push(
      "job_class" => "ProcessPayloadJob",
      "args" => [42],
      "queue" => "critical",
    )
    payload = ProcessPayloadJob.jobs.last

    assert_equal jid, payload["jid"]
    assert_equal "ProcessPayloadJob", payload["class"]
    refute payload.key?("job_class")
    assert_equal "critical", payload["queue"]
  end
end
