# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/redis_test_server"
require_relative "support/redis_fault_proxy"
require_relative "support/reliability_test_support"
require "securerandom"
require "solid_trace"

class ReliabilityMatrixTest < Minitest::Test
  LIFECYCLE = %w[
    job.enqueued
    job.journaled
    job.reserved
    job.started
    job.completed
    job.acknowledged
  ].freeze

  def setup
    super
    SolidJobs.testing!(:disable)
    @redis_config = RedisTestServer.config
    @config = SolidJobs::Blueprint.new(redis: @redis_config, concurrency: 2)
    @config.poll_interval_average = 0.05
    @config.retry_base_delay = 60
    @config.retry_max_delay = 60
    @config.shutdown_timeout = 2
    SolidJobs.use_config(@config)
    @config.redis_pool.call("FLUSHDB")
    @stream = "solid_trace:test:reliability:#{SecureRandom.hex(8)}"
    configure_telemetry
  end

  def teardown
    @server&.stop if @server&.running?
    @proxy&.stop
    SolidTrace.reset!
    @config.close
    super
  end

  def test_nominal_job_has_the_complete_ordered_lifecycle
    result_key = isolated_key("nominal-result")
    @server = SolidJobs::Conductor.new(config: @config).start
    job_id = ReliabilityResultJob.enqueue(result_key, "completed")

    wait_until(5) { @config.redis_pool.call("GET", result_key) == "completed" }
    wait_for_lifecycle(job_id, LIFECYCLE)
    @server.stop

    assert_equal LIFECYCLE, lifecycle_names(job_id), lifecycle_diagnostic(job_id)
    assert_equal "completed", @config.redis_pool.call("GET", result_key)
    assert_job_finished(job_id)
    assert_transport_healthy
  end

  def test_retry_failure_and_success_are_ordered_for_the_same_job
    result_key = isolated_key("retry-result")
    attempts_key = isolated_key("retry-attempts")
    job_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => ReliabilityRetryJob,
      "arguments" => [result_key, attempts_key],
      "channel" => "default",
      "max_failures" => 1,
    )
    @server = SolidJobs::Conductor.new(config: @config).start

    first_attempt = %w[
      job.enqueued job.journaled
      job.reserved job.started job.failed job.retry_scheduled job.acknowledged
    ]
    wait_for_lifecycle(job_id, first_attempt)
    assert_equal 1, SolidJobs::RetryingTasks.new(config: @config).size
    assert_equal 1, SolidJobs::Timer.new(@config).enqueue_due(Time.now.to_f + 120)
    wait_until(5) { @config.redis_pool.call("GET", result_key) == "recovered" }
    expected = %w[
      job.enqueued job.journaled
      job.reserved job.started job.failed job.retry_scheduled job.acknowledged
      job.enqueued job.journaled
      job.reserved job.started job.completed job.acknowledged
    ]
    wait_for_lifecycle(job_id, expected)
    @server.stop

    assert_equal expected, lifecycle_names(job_id), lifecycle_diagnostic(job_id)
    cycles = lifecycle_events(job_id).slice_before do |entry|
      entry.fetch("name") == "job.enqueued"
    end.to_a
    assert_equal(
      [
        first_attempt,
        %w[job.enqueued job.journaled job.reserved job.started job.completed job.acknowledged],
      ],
      cycles.map { |cycle| cycle.map { |entry| entry.fetch("name") } },
    )
    assert_equal [[1], [1]], cycles.map { |cycle| cycle.filter_map { |entry| entry.dig("payload", "attempt") }.uniq }
    assert_equal "2", @config.redis_pool.call("GET", attempts_key)
    assert_equal "recovered", @config.redis_pool.call("GET", result_key)
    assert_job_finished(job_id)
    assert_equal 0, SolidJobs::RetryingTasks.new(config: @config).size
    assert_equal 0, SolidJobs::DiscardedTasks.new(config: @config).size
    assert_transport_healthy
  end

  def test_terminal_failure_is_dead_acknowledged_and_durable
    job_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => ReliabilityTerminalJob,
      "arguments" => [],
      "channel" => "default",
      "max_failures" => 0,
    )
    @server = SolidJobs::Conductor.new(config: @config).start

    expected = %w[
      job.enqueued job.journaled job.reserved job.started
      job.failed job.dead job.acknowledged
    ]
    wait_for_lifecycle(job_id, expected)
    @server.stop

    assert_equal expected, lifecycle_names(job_id), lifecycle_diagnostic(job_id)
    discarded = SolidJobs::DiscardedTasks.new(config: @config).locate(job_id)
    assert_equal 1, discarded.size
    assert_equal 1, discarded.first.envelope.fetch("failure_count")
    assert_equal "ArgumentError", discarded.first.envelope.fetch("exception_type")
    assert_job_finished(job_id, discarded: true)
    assert_transport_healthy
  end

  def test_orphan_recovery_precedes_a_single_business_completion
    job_id = "recovery-#{SecureRandom.hex(8)}"
    result_key = isolated_key("recovery-result")
    effects_key = isolated_key("recovery-effects")
    dead_identity = "#{Socket.gethostname}:999999:#{SecureRandom.hex(4)}"
    claimed_key = SolidJobs::Keyspace.claimed(dead_identity, 0)
    envelope = {
      "id" => job_id,
      "task" => "ReliabilityRecoveryJob",
      "arguments" => [result_key, effects_key],
      "channel" => "default",
      "max_failures" => 0,
    }
    @config.redis_pool.call("LPUSH", claimed_key, JSON.generate(envelope))

    assert_equal 1, SolidJobs::Recovery.call(config: @config).result
    @server = SolidJobs::Conductor.new(config: @config).start
    wait_until(5) { @config.redis_pool.call("GET", result_key) == "completed" }
    expected = %w[job.recovered job.reserved job.started job.completed job.acknowledged]
    wait_for_lifecycle(job_id, expected)
    @server.stop

    assert_equal expected, lifecycle_names(job_id), lifecycle_diagnostic(job_id)
    assert_equal "1", @config.redis_pool.call("GET", effects_key)
    assert_equal 0, @config.redis_pool.call("LLEN", claimed_key)
    assert_job_finished(job_id)
    assert_transport_healthy
  end

  def test_two_ractors_keep_distinct_identity_and_ordered_jobs
    result_keys = 2.times.map { |index| isolated_key("ractor-#{index}") }
    @server = SolidJobs::Conductor.new(config: @config).start
    first_id = ReliabilityBlockingJob.enqueue(result_keys.first)
    wait_until(2) { lifecycle_names(first_id).include?("job.started") }
    second_id = ReliabilityBlockingJob.enqueue(result_keys.last)
    job_ids = [first_id, second_id]

    wait_until(5) { result_keys.all? { |key| @config.redis_pool.call("GET", key) == "completed" } }
    job_ids.each { |job_id| wait_for_lifecycle(job_id, LIFECYCLE) }
    @server.stop

    job_ids.each do |job_id|
      assert_equal LIFECYCLE, lifecycle_names(job_id), lifecycle_diagnostic(job_id)
      assert_job_finished(job_id)
    end
    started = job_ids.map { |job_id| event(job_id, "job.started").fetch("payload") }
    assert_equal [@config.identity], started.map { |payload| payload.fetch("node_id") }.uniq
    assert_equal 2, started.map { |payload| payload.fetch("ractor_id") }.uniq.size
    assert started.all? { |payload| payload.fetch("ractor_id").is_a?(Integer) }
    assert started.all? { |payload| payload.fetch("worker_id") == payload.fetch("ractor_id") }
    assert_transport_healthy
  end

  def test_redis_transport_outage_exposes_loss_staleness_and_recovery
    SolidTrace.reset!
    @exporter.shutdown
    @proxy = RedisFaultProxy.new(@redis_config.server_url).start
    proxy_config = SolidRedis::Config.new(
      url: @proxy.url,
      timeout: 0.05,
      reconnect_attempts: 0,
    )
    configure_telemetry(redis_config: proxy_config)

    SolidTrace.publish("transport.baseline", node_id: @config.identity)
    assert SolidTrace.flush(timeout: 2)
    assert_equal true, @exporter.health.fetch(:connected)
    @proxy.cut!

    outage_key = isolated_key("outage-result")
    @server = SolidJobs::Conductor.new(config: @config).start
    outage_id = ReliabilityResultJob.enqueue(outage_key, "completed-during-outage")
    wait_until(5) { @config.redis_pool.call("GET", outage_key) == "completed-during-outage" }
    wait_until(2) do
      SolidTrace.flush(timeout: 0.1)
      @exporter.health.fetch(:errors).positive?
    end
    failed = @exporter.health
    assert_equal false, failed.fetch(:connected)
    assert_operator failed.fetch(:dropped), :>, 0
    assert_operator failed.fetch(:errors), :>, 0
    assert_operator failed.dig(:last_success, :age_seconds), :>, 0
    refute_nil failed.dig(:last_failure, :at)

    @proxy.restore!
    recovery_key = isolated_key("post-recovery-result")
    recovery_id = ReliabilityResultJob.enqueue(recovery_key, "completed-after-recovery")
    wait_until(5) { @config.redis_pool.call("GET", recovery_key) == "completed-after-recovery" }
    wait_until(3) do
      SolidTrace.flush(timeout: 0.1) &&
        @exporter.health.fetch(:connected)
    end
    wait_for_lifecycle(recovery_id, LIFECYCLE)
    @server.stop

    assert_equal "completed-during-outage", @config.redis_pool.call("GET", outage_key)
    assert_equal "completed-after-recovery", @config.redis_pool.call("GET", recovery_key)
    assert_empty lifecycle_events(outage_id), lifecycle_diagnostic(outage_id)
    assert_equal LIFECYCLE, lifecycle_names(recovery_id), lifecycle_diagnostic(recovery_id)
    recovered = @exporter.health
    assert_equal true, recovered.fetch(:connected)
    assert_operator recovered.fetch(:recoveries), :>=, 1
    assert_operator recovered.dig(:last_failure, :age_seconds), :>=, 0
    assert_operator recovered.fetch(:dropped), :>, 0
    assert_job_finished(outage_id)
    assert_job_finished(recovery_id)
  end

  private

  def configure_telemetry(redis_config: @redis_config)
    @exporter = SolidJobs::RedisStreamsExporter.new(
      redis_config: redis_config,
      stream: @stream,
    )
    SolidTrace.configure do |config|
      config.exporters = [@exporter]
      config.buffer_size = 1_024
      config.batch_size = 1
      config.flush_interval = 0.005
    end
    @config.instrumenter = SolidTrace.instrumenter
  end

  def isolated_key(name)
    "solid-jobs:reliability:#{name}:#{SecureRandom.hex(6)}"
  end

  def wait_for_lifecycle(job_id, expected)
    wait_until(5) do
      SolidTrace.flush(timeout: 0.1)
      lifecycle_names(job_id) == expected
    end
  rescue RuntimeError => error
    flunk("#{error.message}\n#{lifecycle_diagnostic(job_id)}")
  end

  def wait_until(timeout)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not reached in #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end

  def stream_events
    @config.redis_pool.call("XRANGE", @stream, "-", "+").map do |_id, fields|
      JSON.parse(Hash[*fields].fetch("event"))
    end
  end

  def lifecycle_events(job_id)
    stream_events.select { |entry| entry.dig("payload", "job_id") == job_id }
  end

  def lifecycle_names(job_id)
    lifecycle_events(job_id).map { |entry| entry.fetch("name") }
  end

  def lifecycle_diagnostic(job_id)
    JSON.pretty_generate(
      "events" => lifecycle_events(job_id),
      "transport" => @exporter.health,
      "pipeline" => SolidTrace.stats,
    )
  end

  def event(job_id, name)
    lifecycle_events(job_id).find { |entry| entry.fetch("name") == name } ||
      flunk("missing #{name} for #{job_id}: #{lifecycle_diagnostic(job_id)}")
  end

  def assert_job_finished(job_id, discarded: false)
    assert_nil @config.redis_pool.call("HGET", SolidJobs::Keyspace::ATTEMPTS, job_id)
    assert_equal 0, @config.redis_pool.call("LLEN", SolidJobs::Keyspace.channel("default"))
    assert_equal 0, claimed_count
    expected_discarded = discarded ? 1 : 0
    assert_equal expected_discarded, SolidJobs::DiscardedTasks.new(config: @config).locate(job_id).size
  end

  def claimed_count
    scan_keys("solid_jobs:node:*:claimed:*").sum do |key|
      @config.redis_pool.call("LLEN", key)
    end
  end

  def scan_keys(pattern)
    cursor = "0"
    keys = []
    loop do
      cursor, found = @config.redis_pool.call("SCAN", cursor, "MATCH", pattern)
      keys.concat(found)
      break if cursor == "0"
    end
    keys
  end

  def assert_transport_healthy
    assert SolidTrace.flush(timeout: 2)
    health = @exporter.health
    assert_equal true, health.fetch(:connected)
    assert_equal 0, health.fetch(:dropped)
    assert_equal 0, health.fetch(:errors)
    assert_equal health.fetch(:accepted), health.fetch(:exported)
    pipeline = SolidTrace.stats
    assert_equal 0, pipeline.fetch(:dropped)
    assert_equal 0, pipeline.fetch(:errors)
    assert_equal pipeline.fetch(:accepted), pipeline.fetch(:exported)
  end
end
