# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"

class RecoveryRaceTest < Minitest::Test
  include SolidJobsStressHelpers

  CLAIMS = Integer(ENV.fetch("STRESS_RECOVERY_JOBS", "1000"))
  RECOVERERS = Integer(ENV.fetch("STRESS_RECOVERERS", "4"))

  def setup
    @redis_config = RedisTestServer.config
    @config = SolidJobs::Blueprint.new(redis: @redis_config, concurrency: 1)
    use_real_redis!(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
  end

  def test_concurrent_node_recovery_restores_each_claim_once
    envelopes = Array.new(CLAIMS) do |index|
      JSON.generate(
        "task" => "ConcurrentStressJob",
        "arguments" => [index],
        "channel" => "default",
        "id" => "recovery-#{index}",
      )
    end
    identity = "#{Socket.gethostname}:999999:dead"
    envelopes.each_slice(100).with_index do |slice, executor|
      @config.redis_pool.call("LPUSH", SolidJobs::Keyspace.claimed(identity, executor), *slice)
    end

    processes = Array.new(RECOVERERS) do
      fork do
        redis = SolidRedis::Config.new(url: @redis_config.server_url, timeout: 1)
        config = SolidJobs::Blueprint.new(redis: redis, concurrency: 1)
        SolidJobs::Recovery.call(config: config)
        config.close
        exit! 0
      end
    end
    processes.each do |pid|
      Process.wait(pid)
      assert_predicate $?, :success?
    end

    recovered = @config.redis_pool.call("LRANGE", SolidJobs::Keyspace.channel("default"), 0, -1)
      .map { |raw| JSON.parse(raw).fetch("id") }
    assert_equal CLAIMS, recovered.uniq.size
    assert_equal recovered.uniq.size, recovered.size
    assert_empty claimed_keys
  end

  private

  def claimed_keys
    cursor = "0"
    keys = []
    loop do
      cursor, found = @config.redis_pool.call(
        "SCAN", cursor, "MATCH", "#{SolidJobs::Keyspace::PREFIX}:node:*:claimed:*",
      )
      keys.concat(found)
      break if cursor == "0"
    end
    keys
  end
end