# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"

class ConcurrentStressJob
  include SolidJobs::Job
  solid_jobs_options retry: false

  def perform(identifier)
    SolidJobs.redis do |redis|
      redis.pipelined do |pipeline|
        pipeline.call("SADD", "stress:completed", identifier)
        pipeline.call("HINCRBY", "stress:attempts", identifier, 1)
      end
    end
  end
end

class ConcurrentProcessingTest < Minitest::Test
  include SolidJobsStressHelpers

  JOBS = Integer(ENV.fetch("STRESS_JOBS", "10000"))
  PRODUCERS = Integer(ENV.fetch("STRESS_PRODUCERS", "8"))

  def setup
    redis_config = RedisTestServer.config
    @config = SolidJobs::Config.new(redis: redis_config, concurrency: 4)
    SolidJobs.use_config(@config)
    @config.redis_pool.call("FLUSHDB")
    @server = SolidJobs::Server.new(config: @config).start
  end

  def teardown
    @server&.stop if @server&.running?
    @config&.close
  end

  def test_no_job_is_lost_under_concurrent_thread_enqueue
    identifiers = Array.new(JOBS) { SecureRandom.uuid }
    threads = identifiers.each_slice((JOBS.to_f / PRODUCERS).ceil).map do |slice|
      Thread.new do
        slice.each { |identifier| SolidJobs.enqueue("ConcurrentStressJob", [identifier]) }
      end
    end
    threads.each(&:join)

    eventually(timeout: 120) { completed_job_ids.size >= JOBS }

    assert_equal identifiers.sort, completed_job_ids.sort
    assert attempts.values.all? { |count| count == "1" }
    assert_accounted(JOBS)
  end

  def test_many_ractors_enqueue_simultaneously
    ractor_count = Integer(ENV.fetch("STRESS_RACTORS", "8"))
    operations = Integer(ENV.fetch("STRESS_OPS", "5000"))
    redis_config = @config.redis_config
    workers = ractor_count.times.map do |worker_id|
      Ractor.new(worker_id, operations, redis_config) do |id, count, redis|
        local = SolidJobs::Config.new(redis: redis, concurrency: 1)
        SolidJobs.use_config(local)
        count.times do |index|
          SolidJobs.enqueue("ConcurrentStressJob", ["#{id}:#{index}"])
        end
        local.close
        true
      end
    end
    workers.each do |ractor|
      assert_equal true, SolidJobs::RactorSupport.value(ractor)
    end
    expected = ractor_count * operations

    eventually(timeout: 180) { completed_job_ids.size >= expected }

    assert_equal expected, completed_job_ids.uniq.size
    assert attempts.values.all? { |count| count == "1" }
    assert_accounted(expected)
  end

  private

  def completed_job_ids
    Array(@config.redis_pool.call("SMEMBERS", "stress:completed"))
  end

  def attempts
    Array(@config.redis_pool.call("HGETALL", "stress:attempts")).each_slice(2).to_h
  end

  def assert_accounted(enqueued)
    queued = SolidJobs::Queue.all(config: @config).sum(&:size)
    reserved = scan_reserved.sum { |key| @config.redis_pool.call("LLEN", key) }
    dead = SolidJobs::DeadSet.new(config: @config).size
    unique = completed_job_ids.uniq.size

    assert_equal 0, enqueued - unique - queued - reserved - dead, "LOST must equal zero"
  end

  def scan_reserved
    cursor = "0"
    keys = []
    loop do
      cursor, found = @config.redis_pool.call("SCAN", cursor, "MATCH", "*:reserved:*")
      keys.concat(found)
      break if cursor == "0"
    end
    keys
  end
end

