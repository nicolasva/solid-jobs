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
    @config.redis_pool.call("LPUSH", "solid_jobs:channel:default", JSON.generate(ready))
    @config.redis_pool.call("ZADD", SolidJobs::Keyspace::PLANNED, Time.now.to_f + 60, JSON.generate(scheduled))
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
    assert_equal "PLANNED", report.states.fetch("scheduled").first.fetch(:state)
    assert_equal "ACKED", report.states.fetch("acked").first.fetch(:state)
  end

  def test_reports_lost_orphaned_and_duplicate_jobs
    duplicate = JSON.generate(payload("duplicate"))
    @config.redis_pool.call("LPUSH", "solid_jobs:channel:default", duplicate)
    @config.redis_pool.call("ZADD", SolidJobs::Keyspace::RETRIES, Time.now.to_f, duplicate)
    @config.redis_pool.call("LPUSH", "solid_jobs:channel:default", JSON.generate(payload("unknown")))

    report = SolidJobs::IntegrityCheck.call(
      expected_job_ids: %w[missing duplicate],
      config: @config,
    )

    assert_equal ["missing"], report.lost
    assert_equal ["unknown"], report.orphaned
    assert_equal ["duplicate"], report.duplicates.map { |entry| entry.fetch(:job_id) }
    refute report.ok?
  end

  def test_reports_invalid_claims_and_dangling_attempt_indexes
    identity = "host:123:identity"
    claimed_key = SolidJobs::Keyspace.claimed(identity, 4)
    job = payload("reserved")
    @config.redis_pool.call("LPUSH", claimed_key, JSON.generate(job))
    @config.redis_pool.call(
      "HSET",
      SolidJobs::Keyspace.claims(identity),
      "4",
      JSON.generate(
        "task_id" => "wrong",
        "node_id" => identity,
        "executor_id" => "4",
        "channel" => "default",
        "claim_token" => "claim",
        "claimed_at" => Time.now.to_f,
        "attempt" => 1,
      ),
    )
    @config.redis_pool.call("HSET", SolidJobs::Keyspace::ATTEMPTS, "acked", 1)

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

  def test_completion_atomically_removes_claim_metadata_and_attempt_index
    raw = JSON.generate(payload("acknowledged"))
    @config.redis_pool.call("LPUSH", "solid_jobs:channel:default", raw)
    claims = SolidJobs::Claim.new(
      @config,
      identity: "host:123:identity",
      processor_id: 2,
    )

    claim = claims.next

    assert_equal "1", @config.redis_pool.call(
      "HGET",
      SolidJobs::Keyspace::ATTEMPTS,
      "acknowledged",
    )
    assert claim.complete
    assert_nil @config.redis_pool.call("HGET", SolidJobs::Keyspace::ATTEMPTS, "acknowledged")
    assert_equal 0, @config.redis_pool.call(
      "LLEN",
      SolidJobs::Keyspace.claimed("host:123:identity", 2),
    )
    assert_nil @config.redis_pool.call(
      "HGET",
      SolidJobs::Keyspace.claims("host:123:identity"),
      "2",
    )
  end

  def test_stale_claim_cannot_complete_or_requeue_newer_generation
    identity = "host:123:identity"
    metadata_key = SolidJobs::Keyspace.claims(identity)
    claimed_key = SolidJobs::Keyspace.claimed(identity, 2)
    raw = JSON.generate(payload("fenced"))
    @config.redis_pool.call("LPUSH", "solid_jobs:channel:default", raw)
    claim = SolidJobs::Claim.new(
      @config,
      identity: identity,
      processor_id: 2,
    ).next
    metadata = JSON.parse(@config.redis_pool.call("HGET", metadata_key, "2"))
    metadata["claim_token"] = "newer-generation"
    @config.redis_pool.call("HSET", metadata_key, "2", JSON.generate(metadata))

    refute claim.complete
    refute claim.requeue
    assert_equal 1, @config.redis_pool.call("LLEN", claimed_key)
    assert_equal "newer-generation", JSON.parse(
      @config.redis_pool.call("HGET", metadata_key, "2"),
    ).fetch("claim_token")
    assert_equal "1", @config.redis_pool.call(
      "HGET",
      SolidJobs::Keyspace::ATTEMPTS,
      "fenced",
    )
  end

  private

  def payload(job_id)
    {
      "task" => "IntegrityJob",
      "arguments" => [],
      "channel" => "default",
      "id" => job_id,
      "max_failures" => 0,
    }
  end
end
