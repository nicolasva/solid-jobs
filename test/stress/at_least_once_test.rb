# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"

class AtLeastOnceStressTest < Minitest::Test
  include SolidJobsStressHelpers

  def setup
    redis_config = RedisTestServer.config
    @config = SolidJobs::Config.new(redis: redis_config, concurrency: 1)
    use_real_redis!(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
  end

  def test_replayed_job_keeps_the_same_job_id
    # A dedicated queue keeps this test isolated from any Processor that a
    # previous stress test in the same process may still be winding down.
    queue = "at-least-once-#{SecureRandom.hex(4)}"
    job_id = SecureRandom.uuid
    returned = SolidJobs.enqueue(
      "ConcurrentStressJob",
      ["payload"],
      queue: queue,
      job_id: job_id,
    )
    reserved_key = "#{Socket.gethostname}:999999:dead:reserved:0"
    raw = @config.redis_pool.call(
      "LMOVE",
      "queue:#{queue}",
      reserved_key,
      "RIGHT",
      "LEFT",
    )
    reserved = JSON.parse(raw)

    assert_equal job_id, returned
    assert_equal job_id, reserved["jid"]
    assert_equal 1, SolidJobs.recovery!

    replay = JSON.parse(@config.redis_pool.call("LINDEX", "queue:#{queue}", -1))
    assert_equal job_id, replay["jid"]
    assert_equal reserved, replay
  end
end

