# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"

class RecoveryRaceTest < Minitest::Test
  include SolidJobsStressHelpers

  RESERVATIONS = Integer(ENV.fetch("STRESS_RECOVERY_JOBS", "1000"))
  RECOVERERS = Integer(ENV.fetch("STRESS_RECOVERERS", "4"))

  def setup
    @redis_config = RedisTestServer.config
    @config = SolidJobs::Config.new(redis: @redis_config, concurrency: 1)
    use_real_redis!(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
  end

  def test_concurrent_process_recovery_claims_each_reservation_once
    payloads = Array.new(RESERVATIONS) do |index|
      JSON.generate(
        "class" => "ConcurrentStressJob",
        "args" => [index],
        "queue" => "default",
        "jid" => "recovery-#{index}",
      )
    end
    payloads.each_slice(100).with_index do |slice, processor|
      key = "#{Socket.gethostname}:999999:dead:reserved:#{processor}"
      @config.redis_pool.call("LPUSH", key, *slice)
    end

    processes = Array.new(RECOVERERS) do
      fork do
        redis = SolidRedis::Config.new(url: @redis_config.server_url, timeout: 1)
        config = SolidJobs::Config.new(redis: redis, concurrency: 1)
        SolidJobs::Recovery.call(config: config)
        config.close
        exit! 0
      end
    end
    processes.each do |pid|
      Process.wait(pid)
      assert_predicate $?, :success?
    end

    recovered = @config.redis_pool.call("LRANGE", "queue:default", 0, -1)
      .map { |raw| JSON.parse(raw).fetch("jid") }
    assert_equal RESERVATIONS, recovered.uniq.size
    assert_equal recovered.uniq.size, recovered.size
    assert_empty reserved_keys
  end

  private

  def reserved_keys
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
