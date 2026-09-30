# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/redis_test_server"

class IntegrityCheckTest < Minitest::Test
  def setup
    super
    SolidJobs.testing!(:disable)
    @config = SolidJobs::Config.new(redis: RedisTestServer.config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
    super
  end

  def test_accounts_for_ready_scheduled_and_acknowledged_jobs
    ready = payload("ready")
    scheduled = payload("scheduled")
    @config.redis_pool.call("LPUSH", "queue:default", JSON.generate(ready))
    @config.redis_pool.call("ZADD", "schedule", Time.now.to_f + 60, JSON.generate(scheduled))
    @config.redis_pool.call("SADD", "completed", "acked")

    report = SolidJobs::IntegrityCheck.call(
      expected_job_ids: %w[ready scheduled acked],
      acked_key: "completed",
      config: @config,
    )

    assert report.ok?
    assert_empty report.lost
    assert_empty report.duplicates
    assert_equal "READY", report.states.fetch("ready").first.fetch(:state)
    assert_equal "SCHEDULED", report.states.fetch("scheduled").first.fetch(:state)
    assert_equal "ACKED", report.states.fetch("acked").first.fetch(:state)
  end

  def test_reports_lost_orphaned_and_duplicate_jobs
    duplicate = JSON.generate(payload("duplicate"))
    @config.redis_pool.call("LPUSH", "queue:default", duplicate)
    @config.redis_pool.call("ZADD", "retry", Time.now.to_f, duplicate)
    @config.redis_pool.call("LPUSH", "queue:default", JSON.generate(payload("unknown")))

    report = SolidJobs::IntegrityCheck.call(
      expected_job_ids: %w[missing duplicate],
      config: @config,
    )

    assert_equal ["missing"], report.lost
    assert_equal ["unknown"], report.orphaned
    assert_equal ["duplicate"], report.duplicates.map { |entry| entry.fetch(:job_id) }
    refute report.ok?
  end

  def test_reports_invalid_reservations_and_dangling_attempt_indexes
    identity = "host:123:identity"
    reserved_key = "#{identity}:reserved:4"
    job = payload("reserved")
    @config.redis_pool.call("LPUSH", reserved_key, JSON.generate(job))
    @config.redis_pool.call(
      "HSET",
      "#{identity}:reservations",
      "4",
      JSON.generate(
        "job_id" => "wrong",
        "process_id" => identity,
        "worker_id" => "4",
        "queue" => "default",
        "reservation_id" => "reservation",
        "reserved_at" => Time.now.to_f,
        "attempt" => 1,
      ),
    )
    @config.redis_pool.call("HSET", "solid-jobs:attempts", "acked", 1)

    report = SolidJobs::IntegrityCheck.call(
      expected_job_ids: %w[reserved acked],
      acked_job_ids: ["acked"],
      config: @config,
    )

    assert_includes(
      report.invalid_reservations.map { |entry| entry.fetch(:reason) },
      "metadata_mismatch",
    )
    assert_equal ["acked"], report.dangling_indexes.map { |entry| entry.fetch(:job_id) }
    refute report.ok?
  end

  def test_ack_atomically_removes_reservation_metadata_and_attempt_index
    raw = JSON.generate(payload("acknowledged"))
    @config.redis_pool.call("LPUSH", "queue:default", raw)
    fetch = SolidJobs::Fetch.new(
      @config,
      identity: "host:123:identity",
      processor_id: 2,
    )

    work = fetch.retrieve

    assert_equal "1", @config.redis_pool.call(
      "HGET",
      "solid-jobs:attempts",
      "acknowledged",
    )
    assert work.acknowledge
    assert_nil @config.redis_pool.call("HGET", "solid-jobs:attempts", "acknowledged")
    assert_equal 0, @config.redis_pool.call(
      "LLEN",
      "host:123:identity:reserved:2",
    )
    assert_nil @config.redis_pool.call(
      "HGET",
      "host:123:identity:reservations",
      "2",
    )
  end

  def test_stale_reservation_cannot_ack_or_requeue_newer_generation
    identity = "host:123:identity"
    metadata_key = "#{identity}:reservations"
    reserved_key = "#{identity}:reserved:2"
    raw = JSON.generate(payload("fenced"))
    @config.redis_pool.call("LPUSH", "queue:default", raw)
    work = SolidJobs::Fetch.new(
      @config,
      identity: identity,
      processor_id: 2,
    ).retrieve
    metadata = JSON.parse(@config.redis_pool.call("HGET", metadata_key, "2"))
    metadata["reservation_id"] = "newer-generation"
    @config.redis_pool.call("HSET", metadata_key, "2", JSON.generate(metadata))

    refute work.acknowledge
    refute work.requeue
    assert_equal 1, @config.redis_pool.call("LLEN", reserved_key)
    assert_equal "newer-generation", JSON.parse(
      @config.redis_pool.call("HGET", metadata_key, "2"),
    ).fetch("reservation_id")
    assert_equal "1", @config.redis_pool.call(
      "HGET",
      "solid-jobs:attempts",
      "fenced",
    )
  end

  private

  def payload(job_id)
    {
      "class" => "IntegrityJob",
      "args" => [],
      "queue" => "default",
      "jid" => job_id,
      "retry" => false,
    }
  end
end
