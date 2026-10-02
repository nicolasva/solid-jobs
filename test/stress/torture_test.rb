# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"
require_relative "../support/redis_fault_proxy"
require "rbconfig"

class TortureTest < Minitest::Test
  include SolidJobsStressHelpers

  def test_long_running_reliability_with_random_faults
    skip "Set SOLID_JOBS_TORTURE=1 to run" unless ENV["SOLID_JOBS_TORTURE"] == "1"

    total = Integer(ENV.fetch("STRESS_JOBS", "100000"))
    concurrency = Integer(ENV.fetch("STRESS_RACTORS", "4"))
    redis_config = RedisTestServer.config
    config = SolidJobs::Config.new(redis: redis_config, concurrency: 1)
    SolidJobs.use_config(config)
    config.redis_pool.call("FLUSHDB")
    arguments = Array.new(total) { |index| [index] }
    expected_job_ids = SolidJobs.enqueue_bulk(
      "TortureAccountingJob",
      arguments,
      channel: "default",
      batch_size: 1_000,
    )
    proxy = RedisFaultProxy.new(redis_config.server_url).start
    worker = nil
    seed = Integer(ENV.fetch("STRESS_SEED", Random.new_seed.to_s))
    random = Random.new(seed)
    warn "SolidJobs torture seed: #{seed}"
    deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) +
      Float(ENV.fetch("STRESS_TIMEOUT", "1800"))

    until terminally_accounted(config) >= total && active_jobs(config).zero?
      worker ||= start_worker(proxy.url, concurrency)
      sleep 0.05 + random.rand * 0.15
      if random.rand < 0.65
        Process.kill("KILL", worker)
        Process.wait(worker)
        worker = nil
        SolidJobs.recovery!
      else
        proxy.cut!
        sleep 0.01 + random.rand * 0.05
        proxy.restore!
      end
      if ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) >= deadline
        flunk "torture test did not account for #{total} jobs before timeout"
      end
    end

    if worker
      Process.kill("KILL", worker)
      Process.wait(worker)
      worker = nil
    end
    SolidJobs.recovery!
    report = accounting(config, total)
    integrity = SolidJobs::IntegrityCheck.call(
      expected_job_ids: expected_job_ids,
      acked_key: "torture:completed",
      config: config,
    )
    puts JSON.pretty_generate(report)

    assert_equal 0, report.fetch(:lost)
    assert_equal total, report.fetch(:unique_completed)
    assert_empty integrity.lost
    assert_empty integrity.orphaned
    assert_empty integrity.invalid_reservations
    assert_empty integrity.dangling_indexes
    assert_empty integrity.duplicates
    assert_empty integrity.malformed_payloads
  ensure
    if worker
      Process.kill("KILL", worker)
      Process.wait(worker)
    end
    proxy&.stop
    config&.close
  end

  private

  def start_worker(url, concurrency)
    Process.spawn(
      RbConfig.ruby,
      File.expand_path("../support/torture_worker.rb", __dir__),
      url,
      concurrency.to_s,
      out: File::NULL,
      err: File::NULL,
    )
  end

  def terminally_accounted(config)
    unique = config.redis_pool.call("SCARD", "torture:completed")
    dead = SolidJobs::DiscardedTasks.new(config: config).size
    unique + dead
  end

  def accounting(config, enqueued)
    unique = config.redis_pool.call("SCARD", "torture:completed")
    attempts = Array(config.redis_pool.call("HGETALL", "torture:attempts"))
      .each_slice(2).to_h.transform_values!(&:to_i)
    dead = SolidJobs::DiscardedTasks.new(config: config).size
    queued = SolidJobs::Channel.catalog(config: config).sum(&:size)
    reserved = reserved_keys(config).sum { |key| config.redis_pool.call("LLEN", key) }
    {
      enqueued: enqueued,
      unique_completed: unique,
      duplicate_executions: attempts.values.sum { |count| [count - 1, 0].max },
      dead: dead,
      reserved: reserved,
      queued: queued,
      lost: enqueued - unique - dead - reserved - queued,
    }
  end

  def active_jobs(config)
    queued = SolidJobs::Channel.catalog(config: config).sum(&:size)
    reserved = reserved_keys(config).sum { |key| config.redis_pool.call("LLEN", key) }
    scheduled = SolidJobs::PlannedTasks.new(config: config).size
    retries = SolidJobs::RetryingTasks.new(config: config).size
    queued + reserved + scheduled + retries
  end

  def reserved_keys(config)
    cursor = "0"
    keys = []
    loop do
      cursor, found = config.redis_pool.call("SCAN", cursor, "MATCH", "solid_jobs:node:*:claimed:*")
      keys.concat(found)
      break if cursor == "0"
    end
    keys
  end
end
