# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/redis_test_server"
require_relative "support/redis_fault_proxy"
require "rbconfig"

class RedisResultJob
  include SolidJobs::Job

  def perform(value)
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:test-result", value)
  end
end

class GracefulJob
  include SolidJobs::Job

  def perform(duration)
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:graceful-started", "1")
    sleep duration
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:graceful-completed", "1")
  end

  class AfterEffectCrashJob
    include SolidJobs::Job

    def perform
      SolidJobs.config.redis_pool.pipelined do |pipeline|
        pipeline.call("INCR", "solid-jobs:effect-count")
        pipeline.call("HINCRBY", "solid-jobs:effect-attempts", jid, 1)
      end
    end

    class AckWindowJob
      include SolidJobs::Job

      def perform
        SolidJobs.config.redis_pool.call("INCR", "solid-jobs:ack-effect")
      end
    end

    class AckWindowMiddleware
      def call(*)
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
    @config = SolidJobs::Config.new(redis: @redis_config, concurrency: 2)
    SolidJobs.use_config(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
    super
  end

  def test_client_writes_sidekiq_compatible_queue_payload
    jid = RedisResultJob.perform_async(42)
    raw = @config.redis_pool.call("LINDEX", "queue:default", 0)
    payload = JSON.parse(raw)

    assert_includes @config.redis_pool.call("SMEMBERS", "queues"), "default"
    assert_equal jid, payload["jid"]
    assert_equal "RedisResultJob", payload["class"]
    assert_equal [42], payload["args"]
    assert_kind_of Integer, payload["created_at"]
    assert_kind_of Integer, payload["enqueued_at"]
  end

  def test_scheduler_atomically_moves_due_job_to_queue
    jid = SolidJobs::Client.new(config: @config).push(
      "class" => RedisResultJob,
      "args" => [7],
      "queue" => "scheduled",
      "at" => Time.now.to_f + 60,
    )

    assert_equal 1, @config.redis_pool.call("ZCARD", "schedule")
    assert_equal 1, SolidJobs::Scheduler.new(@config).enqueue_due(Time.now.to_f + 61)
    assert_equal 0, @config.redis_pool.call("ZCARD", "schedule")
    payload = JSON.parse(@config.redis_pool.call("LINDEX", "queue:scheduled", 0))
    assert_equal jid, payload["jid"]
  end

  def test_server_processes_job_inside_worker_ractor
    RedisResultJob.perform_async("completed")
    server = SolidJobs::Server.new(config: @config).start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 8

    until @config.redis_pool.call("GET", "solid-jobs:test-result")
      raise "Job was not processed" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.02
    end
    results = server.stop

    assert_equal "completed", @config.redis_pool.call("GET", "solid-jobs:test-result")
    assert_equal 1, results.sum { |result| result.fetch(:processed) }
    assert_equal "1", @config.redis_pool.call("GET", "stat:processed")
  ensure
    server&.stop if server&.running?
  end

  def test_inspection_api_reads_queues_and_sorted_sets
    RedisResultJob.perform_async(9)
    SolidJobs::Client.new(config: @config).push(
      "class" => RedisResultJob,
      "args" => [10],
      "queue" => "later",
      "at" => Time.now.to_f + 60,
    )

    queue = SolidJobs::Queue.all(config: @config).find { |entry| entry.name == "default" }
    scheduled = SolidJobs::ScheduledSet.new(config: @config)

    assert_equal 1, queue.size
    assert_equal "RedisResultJob", queue.first.klass
    assert_operator queue.latency, :>=, 0
    assert_equal 1, scheduled.size
    assert_equal [10], scheduled.first.args
    assert_equal 2, SolidJobs::Stats.new(config: @config).enqueued + scheduled.size
  end

  def test_process_api_observes_and_cleans_heartbeat
    server = SolidJobs::Server.new(config: @config).start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 3
    processes = SolidJobs::ProcessSet.new(config: @config)
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

  def test_retry_and_dead_sets_preserve_failure_payload
    payload = {
      "class" => "RedisResultJob",
      "args" => [1],
      "queue" => "default",
      "jid" => "abcdefabcdefabcdefabcdef",
      "retry" => 1,
      "created_at" => SolidJobs::Utilities.realtime_milliseconds,
    }
    error = RuntimeError.new("failure")

    result = SolidJobs::RetryService.call(config: @config, payload: payload, error: error)
    assert result.successful?
    retry_job = SolidJobs::RetrySet.new(config: @config).first
    assert_equal "RuntimeError", retry_job.item["error_class"]
    assert_equal 0, retry_job.item["retry_count"]

    exhausted = retry_job.item.merge("retry_count" => 0)
    SolidJobs::RetryService.call(config: @config, payload: exhausted, error: error)
    assert_equal 1, SolidJobs::DeadSet.new(config: @config).size
  end

  def test_server_recovers_after_repeated_abrupt_redis_restarts
    server = SolidJobs::Server.new(config: @config).start

    3.times do |index|
      RedisTestServer.interrupt
      sleep 0.15
      RedisTestServer.start
      @config.redis_pool.call("SET", "solid-jobs:test-result", "")
      RedisResultJob.perform_async("cycle-#{index}")
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
    GracefulJob.perform_async(0.2)
    server = SolidJobs::Server.new(config: @config).start
    wait_until(3) { @config.redis_pool.call("GET", "solid-jobs:graceful-started") == "1" }

    server.stop

    assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:graceful-completed")
    assert_equal 0, @config.redis_pool.call("LLEN", "queue:default")
  end

  def test_shutdown_timeout_interrupts_and_requeues_running_job
    @config.shutdown_timeout = 0.05
    GracefulJob.perform_async(2)
    server = SolidJobs::Server.new(config: @config).start
    wait_until(3) { @config.redis_pool.call("GET", "solid-jobs:graceful-started") == "1" }

    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    server.stop
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, 1
    assert_equal 1, @config.redis_pool.call("LLEN", "queue:default")
    assert_nil @config.redis_pool.call("GET", "solid-jobs:graceful-completed")
  end

  def test_job_reserved_by_killed_process_is_recovered
    SolidJobs::Client.new(config: @config).push(
      "class" => "CrashWorkerJob",
      "args" => [],
      "queue" => "default",
      "retry" => false,
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
    reserved = scan_keys("*:reserved:*")
    assert_equal 1, reserved.length
    assert_equal 1, @config.redis_pool.call("LLEN", reserved.first)

    Process.kill("KILL", pid)
    Process.wait(pid)
    pid = nil
    result = SolidJobs::Recovery.call(config: @config)

    assert_equal 1, result.result
    assert_equal 1, @config.redis_pool.call("LLEN", "queue:default")
    assert_equal 0, @config.redis_pool.call("LLEN", reserved.first)
  ensure
    if pid
      Process.kill("KILL", pid)
      Process.wait(pid)
    end

    def test_crash_after_application_effect_before_ack_replays_job
      job_id = SecureRandom.uuid
      SolidJobs::Client.new(config: @config).push(
        "class" => "AfterEffectCrashJob",
        "args" => [],
        "queue" => "default",
        "jid" => job_id,
        "retry" => false,
      )
      script = File.expand_path("support/crash_worker.rb", __dir__)
      pid = Process.spawn(
        RbConfig.ruby,
        script,
        @redis_config.server_url,
        "after-perform",
        out: File::NULL,
        err: File::NULL,
      )
      wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:after-perform") == "1" }
      assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:effect-count")
      assert_equal 1, scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }

      Process.kill("KILL", pid)
      Process.wait(pid)
      pid = nil
      SolidJobs::Recovery.call(config: @config)
      recovered = JSON.parse(@config.redis_pool.call("LINDEX", "queue:default", -1))
      assert_equal job_id, recovered["jid"]
      server = SolidJobs::Server.new(config: @config).start
      wait_until(5) { @config.redis_pool.call("HGET", "solid-jobs:effect-attempts", job_id) == "2" }
      server.stop

      assert_equal "2", @config.redis_pool.call("GET", "solid-jobs:effect-count")
      assert_equal "2", @config.redis_pool.call("HGET", "solid-jobs:effect-attempts", job_id)
      assert_equal 0, scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }
    ensure
      server&.stop if server&.running?
      if pid
        Process.kill("KILL", pid)
        Process.wait(pid)
      end

      def test_two_recoverers_atomically_restore_each_job_once
        identity = "#{Socket.gethostname}:999999:dead"
        reserved = "#{identity}:reserved:0"
        expected = 100.times.map do |index|
          JSON.generate(
            "class" => "RedisResultJob",
            "args" => [index],
            "queue" => "default",
            "jid" => format("%024x", index),
          )
        end
        @config.redis_pool.call("LPUSH", reserved, *expected)

        results = 2.times.map do
          Thread.new { SolidJobs::Recovery.call(config: @config).result }
        end.map(&:value)
        restored = @config.redis_pool.call("LRANGE", "queue:default", 0, -1)

        assert_equal 100, results.sum
        assert_equal 100, restored.length
        assert_equal expected.sort, restored.sort
        assert_equal 0, @config.redis_pool.call("LLEN", reserved)
      end

      def test_collective_process_crash_recovers_all_reserved_jobs
        12.times do |index|
          SolidJobs::Client.new(config: @config).push(
            "class" => "CrashWorkerJob",
            "args" => [],
            "queue" => "default",
            "jid" => format("%024x", index),
            "retry" => false,
          )
        end
        script = File.expand_path("support/crash_worker.rb", __dir__)
        pid = Process.spawn(
          RbConfig.ruby,
          script,
          @redis_config.server_url,
          "during-perform",
          "4",
          out: File::NULL,
          err: File::NULL,
        )
        wait_until(5) { scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) } == 4 }

        Process.kill("KILL", pid)
        Process.wait(pid)
        pid = nil
        recovered = SolidJobs::Recovery.call(config: @config).result
        total = @config.redis_pool.call("LLEN", "queue:default") +
          scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }

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
        proxy_config = SolidJobs::Config.new(redis: proxy_redis, concurrency: 1)
        proxy_config.server_middleware.add(AckWindowMiddleware)
        SolidJobs.use_config(proxy_config)
        AckWindowJob.perform_async
        server = SolidJobs::Server.new(config: proxy_config).start
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-window") == "1" }
        proxy.cut!
        sleep 0.4
        proxy.restore!
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-effect") == "2" }
        server.stop

        assert_equal "2", @config.redis_pool.call("GET", "solid-jobs:ack-effect")
        assert_equal 0, scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }
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
        proxy_config = SolidJobs::Config.new(redis: proxy_redis, concurrency: 1)
        SolidJobs.use_config(proxy_config)
        proxy.drop_next_response_for!("LREM")
        AckWindowJob.perform_async
        server = SolidJobs::Server.new(config: proxy_config).start
        wait_until(5) { @config.redis_pool.call("GET", "solid-jobs:ack-effect") == "1" }
        sleep 0.5
        server.stop

        assert_equal "1", @config.redis_pool.call("GET", "solid-jobs:ack-effect")
        assert_equal 0, scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }
        assert_equal 0, @config.redis_pool.call("LLEN", "queue:default")
      ensure
        server&.stop if server&.running?
        proxy_config&.close
        proxy&.stop
        SolidJobs.use_config(@config)
      end

      def test_backpressure_limits_reservations_to_processor_capacity
        @config.concurrency = 4
        100.times do |index|
          SolidJobs::Client.new(config: @config).push(
            "class" => GracefulJob,
            "args" => [1],
            "queue" => "default",
            "jid" => format("%024x", index),
            "retry" => false,
          )
        end
        server = SolidJobs::Server.new(config: @config).start
        wait_until(3) do
          scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) } == 4
        end

        reserved = scan_keys("*:reserved:*").sum { |key| @config.redis_pool.call("LLEN", key) }
        queued = @config.redis_pool.call("LLEN", "queue:default")
        assert_equal 4, reserved
        assert_operator queued, :>=, 96
      ensure
        server&.stop if server&.running?
      end
    end
  end

  private

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
