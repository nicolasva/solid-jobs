# frozen_string_literal: true

require_relative "test_helper"
require_relative "support/redis_test_server"
require_relative "stress/stress_test_helper"

class SoakJob
  include SolidJobs::Job

  def perform(index)
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:soak:last", index)
  end
end

class SoakTest < Minitest::Test
  include SolidJobsStressHelpers

  def test_sustained_real_redis_workload
    skip "Set SOLID_JOBS_SOAK=1 to run" unless ENV["SOLID_JOBS_SOAK"] == "1"

    duration = Float(ENV.fetch("SOLID_JOBS_SOAK_SECONDS", "86400"))
    batch_size = Integer(ENV.fetch("SOLID_JOBS_SOAK_BATCH", "100"))
    redis_config = RedisTestServer.config
    config = SolidJobs::Config.new(redis: redis_config, concurrency: 4)
    SolidJobs.use_config(config)
    config.redis_pool.call("FLUSHDB")
    server = SolidJobs::Server.new(config: config).start
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + duration
    baseline_slots = GC.stat(:heap_live_slots)
    submitted = 0

    while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
      arguments = Array.new(batch_size) { |offset| [submitted + offset] }
      SoakJob.perform_bulk(arguments)
      submitted += batch_size
      sleep 0.01 while Integer(config.redis_pool.call("LLEN", "queue:default")) > batch_size * 10
    end

    sleep 0.01 until Integer(config.redis_pool.call("GET", "stat:processed") || 0) >= submitted
    server.stop
    GC.start
    growth = GC.stat(:heap_live_slots) - baseline_slots

    assert_equal submitted - 1, Integer(config.redis_pool.call("GET", "solid-jobs:soak:last"))
    assert_operator growth, :<, Integer(ENV.fetch("SOLID_JOBS_SOAK_MAX_HEAP_GROWTH", "25000"))
  ensure
    server&.stop if server&.running?
    config&.close
  end
end
