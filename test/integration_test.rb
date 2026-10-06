# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/redis_test_server"
require_relative "support/redis_fault_proxy"
require_relative "support/reliability_test_support"
require "rbconfig"

class FailingTelemetryInstrumenter
  def self.instrument(_name, _payload)
    raise "telemetry unavailable"
  end
end

class SlowTelemetryInstrumenter
  def self.instrument(name, payload)
    return unless name == "job.started"

    SolidJobs.config.redis_pool.call("SET", "solid-jobs:slow-instrumenter", payload.fetch(:job_id))
    sleep 5
  end
end

class GracefulJob
  include SolidJobs::Task

  def execute_task(duration)
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:graceful-started", "1")
    sleep duration
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:graceful-completed", "1")
  end

  class AfterEffectCrashJob
    include SolidJobs::Task

    def execute_task
      SolidJobs.config.redis_pool.pipelined do |pipeline|
        pipeline.call("INCR", "solid-jobs:effect-count")
        pipeline.call("HINCRBY", "solid-jobs:effect-attempts", task_id, 1)
      end
    end

    class AckWindowJob
      include SolidJobs::Task

      def execute_task
        SolidJobs.config.redis_pool.call("INCR", "solid-jobs:ack-effect")
      end
    end

    class AckWindowInterceptor
      def around(_context)
        result = yield
        SolidJobs.config.redis_pool.call("SET", "solid-jobs:ack-window", "1")
        sleep 0.2
        result
      end
    end
  end
end

class IntegrationTest < Minitest::Test
  def setup
    super
    SolidJobs.testing!(:disable)
    @redis_config = RedisTestServer.config
    @config = SolidJobs::Blueprint.new(redis: @redis_config, concurrency: 2)
    SolidJobs.use_config(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
    super
  end

  def test_publisher_writes_solid_jobs_envelope
    task_id = RedisResultJob.enqueue(42)
    raw = @config.redis_pool.call("LINDEX", "solid_jobs:channel:default", 0)
    payload = JSON.parse(raw)

    assert_includes @config.redis_pool.call("SMEMBERS", SolidJobs::Keyspace::CHANNELS), "default"
    assert_equal task_id, payload["id"]
    assert_equal "RedisResultJob", payload["task"]
    assert_equal [42], payload["arguments"]
    assert_kind_of Integer, payload["created_ms"]
    assert_kind_of Integer, payload["queued_ms"]
  end

  def test_scheduler_atomically_moves_due_job_to_queue
    task_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => RedisResultJob,
      "arguments" => [7],
      "channel" => "scheduled",
      "run_at" => Time.now.to_f + 60,
    )

    assert_equal 1, @config.redis_pool.call("ZCARD", SolidJobs::Keyspace::PLANNED)
    assert_equal 1, SolidJobs::Timer.new(@config).enqueue_due(Time.now.to_f + 61)
    assert_equal 0, @config.redis_pool.call("ZCARD", SolidJobs::Keyspace::PLANNED)
    payload = JSON.parse(@config.redis_pool.call("LINDEX", "solid_jobs:channel:scheduled", 0))
    assert_equal task_id, payload["id"]
  end

  def test_server_processes_job_inside_worker_ractor
    RedisResultJob.enqueue("completed")
    server = SolidJobs::Conductor.new(config: @config).start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 8

    until @config.redis_pool.call("GET", "solid-jobs:test-result")
      raise "Job was not processed" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.02
    end

    results = server.stop

    assert_equal "completed", @config.redis_pool.call("GET", "solid-jobs:test-result")
    assert_equal 1, results.sum { |result| result.fetch(:processed) }
    assert_equal "1", @config.redis_pool.call("GET", "solid_jobs:metrics:processed")
  ensure
    server&.stop if server&.running?
  end

  def test_successful_job_emits_the_ordered_lifecycle_without_business_arguments
    @config.instrumenter = RedisTelemetryInstrumenter
    server = SolidJobs::Conductor.new(config: @config).start
    job_id = RedisResultJob.enqueue("telemetry-success")
    wait_until(5) { telemetry_names(job_id).include?("job.acknowledged") }
    server.stop

    events = telemetry_events(job_id)
    names = events.map { |event| event.fetch("name") }
    assert_equal(
      %w[job.enqueued job.journaled],
      names.select { |name| %w[job.enqueued job.journaled].include?(name) },
    )
    assert_equal(
      %w[job.reserved job.started job.completed job.acknowledged],
      names.select do |name|
        %w[job.reserved job.started job.completed job.acknowledged].include?(name)
      end,
    )
    payloads = events.map { |event| event.fetch("payload") }
    assert payloads.all? { |payload| payload.fetch("node_id") == @config.identity }
    assert payloads.none? { |payload| payload.key?("arguments") || payload.key?("log") }
    worker_payloads = payloads.select { |payload| payload.key?("ractor_id") }
    assert worker_payloads.all? { |payload| payload["ractor_id"].is_a?(Integer) }
    assert worker_payloads.all? { |payload| payload["worker_id"] == payload["ractor_id"] }
    assert events.find { |event| event["name"] == "job.completed" }.fetch("payload").fetch("duration") >= 0
  ensure
    server&.stop if server&.running?
  end

  def test_retryable_and_terminal_failures_emit_retry_dead_and_acknowledgement
    @config.instrumenter = RedisTelemetryInstrumenter
    retry_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => TelemetryFailureJob,
      "arguments" => [],
      "channel" => "default",
      "max_failures" => 1,
    )
    dead_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => TelemetryFailureJob,
      "arguments" => [],
      "channel" => "default",
      "max_failures" => 0,
    )
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(5) do
      telemetry_names(retry_id).include?("job.acknowledged") &&
        telemetry_names(dead_id).include?("job.acknowledged")
    end
    server.stop

    assert_equal(
      %w[job.enqueued job.journaled job.reserved job.started job.failed job.retry_scheduled job.acknowledged],
      telemetry_names(retry_id),
    )
    assert_equal(
      %w[job.enqueued job.journaled job.reserved job.started job.failed job.dead job.acknowledged],
      telemetry_names(dead_id),
    )
    failed = telemetry_events(dead_id).find { |event| event["name"] == "job.failed" }.fetch("payload")
    assert_equal "ArgumentError", failed.fetch("error_class")
    assert_equal "expected failure", failed.fetch("error_message")
    assert_equal 1, failed.fetch("attempt")
  ensure
    server&.stop if server&.running?
  end

  def test_recovery_emits_only_after_the_job_is_restored
    @config.instrumenter = RedisTelemetryInstrumenter
    identity = "#{Socket.gethostname}:999999:dead"
    claimed = SolidJobs::Keyspace.claimed(identity, 0)
    envelope = {
      "id" => "recovered-job",
      "task" => "RedisResultJob",
      "arguments" => ["secret"],
      "channel" => "default",
    }
    @config.redis_pool.call("LPUSH", claimed, JSON.generate(envelope))

    assert_equal 1, SolidJobs::Recovery.call(config: @config).result

    assert_equal ["job.recovered"], telemetry_names("recovered-job")
    restored = JSON.parse(@config.redis_pool.call("LINDEX", SolidJobs::Keyspace.channel("default"), 0))
    assert_equal "recovered-job", restored.fetch("id")
  end

  def test_failing_instrumenter_does_not_change_success_or_terminal_failure
    @config.concurrency = 1
    @config.instrumenter = FailingTelemetryInstrumenter
    success_id = RedisResultJob.enqueue("instrumenter-failed")
    failure_id = SolidJobs::Publisher.new(config: @config).publish(
      "task" => TelemetryFailureJob,
      "arguments" => [],
      "channel" => "default",
      "max_failures" => 0,
    )
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(5) do
      @config.redis_pool.call("GET", "solid-jobs:test-result") == "instrumenter-failed" &&
        SolidJobs::DiscardedTasks.new(config: @config).to_a.any? { |task| task.id == failure_id }
    end
    results = server.stop

    assert_equal "instrumenter-failed", @config.redis_pool.call("GET", "solid-jobs:test-result")
    assert_equal 1, results.sum { |result| result.fetch(:processed) }
    assert_equal 1, results.sum { |result| result.fetch(:failed) }
    assert_nil @config.redis_pool.call("HGET", SolidJobs::Keyspace::ATTEMPTS, success_id)
  ensure
    server&.stop if server&.running?
  end

  def test_worker_ractors_share_node_identity_and_use_distinct_integer_ids
    @config.instrumenter = RedisTelemetryInstrumenter
    job_ids = 2.times.map { TelemetryBlockingJob.enqueue }
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(5) do
      job_ids.sum { |job_id| telemetry_names(job_id).count("job.started") } == 2
    end
    server.stop

    started = job_ids.map do |job_id|
      telemetry_events(job_id).find { |event| event["name"] == "job.started" }.fetch("payload")
    end
    assert_equal [@config.identity], started.map { |payload| payload.fetch("node_id") }.uniq
    assert_equal [0, 1], started.map { |payload| payload.fetch("ractor_id") }.sort
    assert started.all? { |payload| payload["worker_id"] == payload["ractor_id"] }
  ensure
    server&.stop if server&.running?
  end

  def test_heartbeat_emits_process_worker_and_redis_observations
    @config.instrumenter = RedisTelemetryInstrumenter
    job_id = TelemetryBlockingJob.enqueue
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(5) do
      observation_events.map { |event| event.fetch("name") }.uniq.sort ==
        %w[process.observed ractor.observed redis.observed].sort &&
        telemetry_names(job_id).include?("job.started")
    end
    wait_until(5) { telemetry_names(job_id).include?("job.acknowledged") }
    before_quiet = observation_events.count { |event| event.fetch("name") == "ractor.observed" }
    server.quiet
    wait_until(2) do
      observation_events.count { |event| event.fetch("name") == "ractor.observed" } > before_quiet
    end
    server.stop

    events = observation_events
    assert events.all? { |event| event.dig("payload", "node_id") == @config.identity }
    ractors = events.select { |event| event.fetch("name") == "ractor.observed" }
    assert_equal [0, 1], ractors.map { |event| event.dig("payload", "ractor_id") }.uniq.sort
    assert(ractors.any? do |event|
      event.dig("payload", "state") == "busy" &&
        event.dig("payload", "current_job_ids") == [job_id]
    end, ractors.inspect)
    redis = events.select { |event| event.fetch("name") == "redis.observed" }
    assert redis.any? { |event| event.dig("payload", "connection_status") == "connected" }
    assert(redis.all? do |event|
      event.dig("payload", "metrics").values.all? { |metric| metric["status"] == "unavailable" }
    end)
    states = ractors.map { |event| event.dig("payload", "state") }
    activities = ractors.map { |event| event.dig("payload", "activity") }
    assert_includes activities, "waiting"
    assert_includes states, "stopping"
    assert_includes states, "stopped"
  ensure
    server&.stop if server&.running?
  end

  def test_inspection_api_reads_channels_and_timed_tasks
    RedisResultJob.enqueue(9)
    SolidJobs::Publisher.new(config: @config).publish(
      "task" => RedisResultJob,
      "arguments" => [10],
      "channel" => "later",
      "run_at" => Time.now.to_f + 60,
    )

    channel = SolidJobs::Channel.catalog(config: @config).find { |entry| entry.name == "default" }
    planned = SolidJobs::PlannedTasks.new(config: @config)

    assert_equal 1, channel.size
    assert_equal "RedisResultJob", channel.first.task
    assert_operator channel.waiting_time, :>=, 0
    assert_equal 1, planned.size
    assert_equal [10], planned.first.arguments
    assert_equal 2, SolidJobs::Counters.new(config: @config).ready + planned.size
  end

  def test_node_control_requests_use_the_namespaced_mailbox
    node = SolidJobs::Node.new("control-test", config: @config)
    mailbox = SolidJobs::Keyspace.node_signals(node.identity)

    {pause: "TSTP", shutdown: "TERM", backtraces: "TTIN"}.each do |action, expected|
      assert node.request_control(action)
      assert_equal expected, @config.redis_pool.call("RPOP", mailbox)
    end
  end

  def test_unknown_node_control_requests_are_rejected_without_writing
    node = SolidJobs::Node.new("control-test", config: @config)

    assert_raises(KeyError) { node.request_control(:invalid) }
    assert_equal 0, @config.redis_pool.call("LLEN", SolidJobs::Keyspace.node_signals(node.identity))
  end

  def test_process_api_observes_and_cleans_heartbeat
    server = SolidJobs::Conductor.new(config: @config).start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
    processes = SolidJobs::Nodes.new(config: @config)
    sleep 0.01 while processes.to_a.empty? &&
      Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

    process = processes.first
    refute_nil process
    assert_equal server.identity, process.identity
    assert_equal 2, process.concurrency

    server.stop
    assert_empty processes.to_a
  ensure
    server&.stop if server&.running?
  end

  def test_retrying_and_discarded_tasks_preserve_failure_envelope
    payload = {
      "task" => "RedisResultJob",
      "arguments" => [1],
      "channel" => "default",
      "id" => "abcdefabcdefabcdefabcdef",
      "max_failures" => 1,
      "created_ms" => SolidJobs::Utilities.realtime_milliseconds,
    }
    error = RuntimeError.new("failure")

    result = SolidJobs::FailurePolicy.call(config: @config, payload: payload, error: error)
    assert result.successful?
    retrying_task = SolidJobs::RetryingTasks.new(config: @config).first
    assert_equal "RuntimeError", retrying_task.envelope["exception_type"]
    assert_equal 1, retrying_task.envelope["failure_count"]

    exhausted = retrying_task.envelope
    SolidJobs::FailurePolicy.call(config: @config, payload: exhausted, error: error)
    assert_equal 1, SolidJobs::DiscardedTasks.new(config: @config).size
  end

  def test_server_recovers_after_repeated_abrupt_redis_restarts
    server = SolidJobs::Conductor.new(config: @config).start

    3.times do |index|
      RedisTestServer.interrupt
      sleep 0.15
      RedisTestServer.start
      @config.redis_pool.call("SET", "solid-jobs:test-result", "")
      RedisResultJob.enqueue("cycle-#{index}")
      wait_until(8) do
        @config.redis_pool.call("GET", "solid-jobs:test-result") == "cycle-#{index}"
      rescue SolidRedis::ConnectionError
        false
      end
    end

    assert server.running?
  ensure
    server&.stop if server&.running?
  end

  def test_graceful_shutdown_waits_for_running_job
    @config.shutdown_timeout = 2
    GracefulJob.enqueue(0.2)
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(3) { @config.redis_pool.call("GET", "solid-jobs:graceful-started") == "1" }

    server.stop

    assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:graceful-completed")
    assert_equal 0, @config.redis_pool.call("LLEN", "solid_jobs:channel:default")
  end

  def test_shutdown_timeout_interrupts_and_requeues_running_job
    @config.shutdown_timeout = 0.05
    @config.instrumenter = RedisTelemetryInstrumenter
    job_id = GracefulJob.enqueue(2)
    server = SolidJobs::Conductor.new(config: @config).start
    wait_until(3) { @config.redis_pool.call("GET", "solid-jobs:graceful-started") == "1" }

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.stop
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, 1
    assert_equal 1, @config.redis_pool.call("LLEN", "solid_jobs:channel:default")
    assert_nil @config.redis_pool.call("GET", "solid-jobs:graceful-completed")
    names = telemetry_names(job_id)
    assert_equal %w[job.enqueued job.journaled], names.first(2)
    assert_includes names, "job.reserved"
    assert_includes names, "job.started"
    assert_empty names & %w[job.completed job.failed job.acknowledged]
  end

  def test_shutdown_interrupt_during_started_instrumentation_requeues_the_claim
    @config.shutdown_timeout = 0.05
    @config.instrumenter = SlowTelemetryInstrumenter
    job_id = RedisResultJob.enqueue("halted-instrumentation")
    server = SolidJobs::Conductor.new(config: @config).start

    wait_until(3) { @config.redis_pool.call("GET", "solid-jobs:slow-instrumenter") == job_id }
    results = server.stop

    assert_equal 0, results.sum { |result| result.fetch(:processed) }
    assert_equal 0, results.sum { |result| result.fetch(:failed) }
    assert_nil @config.redis_pool.call("GET", "solid-jobs:test-result")
    queued = JSON.parse(@config.redis_pool.call("LINDEX", "solid_jobs:channel:default", 0))
    assert_equal job_id, queued.fetch("id")
  ensure
    server&.stop if server&.running?
  end

  def test_job_reserved_by_killed_process_is_recovered
    SolidJobs::Publisher.new(config: @config).publish(
      "task" => "CrashWorkerJob",
      "arguments" => [],
      "channel" => "default",
      "max_failures" => 0,
    )
    script = File.expand_path("support/crash_worker.rb", __dir__)
    pid = Process.spawn(
      RbConfig.ruby,
      script,
      @redis_config.server_url,
      out: File::NULL,
      err: File::NULL,
    )
    wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:crash-started") == "1" }
    reserved = scan_keys("solid_jobs:node:*:claimed:*")
    assert_equal 1, reserved.length
    assert_equal 1, @config.redis_pool.call("LLEN", reserved.first)

    Process.kill("KILL", pid)
    Process.wait(pid)
    pid = nil
    result = SolidJobs::Recovery.call(config: @config)

    assert_equal 1, result.result
    assert_equal 1, @config.redis_pool.call("LLEN", "solid_jobs:channel:default")
    assert_equal 0, @config.redis_pool.call("LLEN", reserved.first)
  ensure
    if pid
      Process.kill("KILL", pid)
      Process.wait(pid)
    end

    def test_crash_after_application_effect_before_ack_replays_job
      job_id = SecureRandom.uuid
      SolidJobs::Publisher.new(config: @config).publish(
        "task" => "AfterEffectCrashJob",
        "arguments" => [],
        "channel" => "default",
        "id" => job_id,
        "max_failures" => 0,
      )
      script = File.expand_path("support/crash_worker.rb", __dir__)
      pid = Process.spawn(
        RbConfig.ruby,
        script,
        @redis_config.server_url,
        "after-execution",
        out: File::NULL,
        err: File::NULL,
      )
      wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:after-execution") == "1" }
      assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:effect-count")
      assert_equal 1, scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }

      Process.kill("KILL", pid)
      Process.wait(pid)
      pid = nil
      SolidJobs::Recovery.call(config: @config)
      recovered = JSON.parse(@config.redis_pool.call("LINDEX", "solid_jobs:channel:default", -1))
      assert_equal job_id, recovered["id"]
      server = SolidJobs::Conductor.new(config: @config).start
      wait_until(5) { @config.redis_pool.call("HGET", "solid-jobs:effect-attempts", job_id) == "2" }
      server.stop

      assert_equal "2", @config.redis_pool.call("GET", "solid-jobs:effect-count")
      assert_equal "2", @config.redis_pool.call("HGET", "solid-jobs:effect-attempts", job_id)
      assert_equal 0, scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }
    ensure
      server&.stop if server&.running?
      if pid
        Process.kill("KILL", pid)
        Process.wait(pid)
      end

      def test_two_recoverers_atomically_restore_each_job_once
        identity = "#{Socket.gethostname}:999999:dead"
        reserved = SolidJobs::Keyspace.claimed(identity, 0)
        expected = 100.times.map do |index|
          JSON.generate(
            "task" => "RedisResultJob",
            "arguments" => [index],
            "channel" => "default",
            "id" => format("%024x", index),
          )
        end
        @config.redis_pool.call("LPUSH", reserved, *expected)

        results = 2.times.map do
          Thread.new { SolidJobs::Recovery.call(config: @config).result }
        end.map(&:value)
        restored = @config.redis_pool.call("LRANGE", "solid_jobs:channel:default", 0, -1)

        assert_equal 100, results.sum
        assert_equal 100, restored.length
        assert_equal expected.sort, restored.sort
        assert_equal 0, @config.redis_pool.call("LLEN", reserved)
      end

      def test_collective_process_crash_recovers_all_reserved_jobs
        12.times do |index|
          SolidJobs::Publisher.new(config: @config).publish(
            "task" => "CrashWorkerJob",
            "arguments" => [],
            "channel" => "default",
            "id" => format("%024x", index),
            "max_failures" => 0,
          )
        end
        script = File.expand_path("support/crash_worker.rb", __dir__)
        pid = Process.spawn(
          RbConfig.ruby,
          script,
          @redis_config.server_url,
          "during-execution",
          "4",
          out: File::NULL,
          err: File::NULL,
        )
        wait_until(5) { scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) } == 4 }

        Process.kill("KILL", pid)
        Process.wait(pid)
        pid = nil
        recovered = SolidJobs::Recovery.call(config: @config).result
        total = @config.redis_pool.call("LLEN", "solid_jobs:channel:default") +
          scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }

        assert_equal 4, recovered
        assert_equal 12, total
      ensure
        if pid
          Process.kill("KILL", pid)
          Process.wait(pid)
        end
      end

      def test_connection_lost_before_ack_replays_reserved_job
        proxy = RedisFaultProxy.new(@redis_config.server_url).start
        proxy_redis = SolidRedis::Config.new(
          url: proxy.url,
          timeout: 0.1,
          reconnect_attempts: 0,
        )
        proxy_config = SolidJobs::Blueprint.new(redis: proxy_redis, concurrency: 1)
        proxy_config.execute_interceptors.use(AckWindowInterceptor)
        SolidJobs.use_config(proxy_config)
        AckWindowJob.enqueue
        server = SolidJobs::Conductor.new(config: proxy_config).start
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-window") == "1" }
        proxy.cut!
        sleep 0.4
        proxy.restore!
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-effect") == "2" }
        server.stop

        assert_equal "2", @config.redis_pool.call("GET", "solid-jobs:ack-effect")
        assert_equal 0, scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }
      ensure
        server&.stop if server&.running?
        proxy_config&.close
        proxy&.stop
        SolidJobs.use_config(@config)
      end

      def test_ack_executed_but_response_lost_does_not_lose_or_replay_job
        proxy = RedisFaultProxy.new(@redis_config.server_url).start
        proxy_redis = SolidRedis::Config.new(
          url: proxy.url,
          timeout: 0.1,
          reconnect_attempts: 0,
        )
        proxy_config = SolidJobs::Blueprint.new(redis: proxy_redis, concurrency: 1)
        SolidJobs.use_config(proxy_config)
        proxy.drop_next_response_for!("LREM")
        AckWindowJob.enqueue
        server = SolidJobs::Conductor.new(config: proxy_config).start
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-effect") == "1" }
        sleep 0.5
        server.stop

        assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:ack-effect")
        assert_equal 0, scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }
        assert_equal 0, @config.redis_pool.call("LLEN", "solid_jobs:channel:default")
      ensure
        server&.stop if server&.running?
        proxy_config&.close
        proxy&.stop
        SolidJobs.use_config(@config)
      end

      def test_backpressure_limits_reservations_to_processor_capacity
        @config.concurrency = 4
        100.times do |index|
          SolidJobs::Publisher.new(config: @config).publish(
            "task" => GracefulJob,
            "arguments" => [1],
            "channel" => "default",
            "id" => format("%024x", index),
            "max_failures" => 0,
          )
        end
        server = SolidJobs::Conductor.new(config: @config).start
        wait_until(3) do
          scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) } == 4
        end

        reserved = scan_keys("solid_jobs:node:*:claimed:*").sum { |key| @config.redis_pool.call("LLEN", key) }
        queued = @config.redis_pool.call("LLEN", "solid_jobs:channel:default")
        assert_equal 4, reserved
        assert_operator queued, :>=, 96
      ensure
        server&.stop if server&.running?
      end
    end
  end

  private

  def telemetry_events(job_id)
    @config.redis_pool.call("LRANGE", RedisTelemetryInstrumenter::KEY, 0, -1)
      .map { |raw| JSON.parse(raw) }
      .select { |event| event.fetch("payload")["job_id"] == job_id }
  end

  def observation_events
    @config.redis_pool.call("LRANGE", RedisTelemetryInstrumenter::KEY, 0, -1)
      .map { |raw| JSON.parse(raw) }
      .select { |event| event.fetch("name").end_with?(".observed") }
  end

  def telemetry_names(job_id)
    telemetry_events(job_id).map { |event| event.fetch("name") }
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

  def wait_until(timeout)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    until yield
      raise "condition not reached in #{timeout}s" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end

end
