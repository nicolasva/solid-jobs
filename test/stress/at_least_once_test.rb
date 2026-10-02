# frozen_string_literal: true

require_relative "stress_test_helper"
require_relative "../support/redis_test_server"

class AtLeastOnceStressTest < Minitest::Test
  include SolidJobsStressHelpers

  def setup
    redis_config = RedisTestServer.config
    @config = SolidJobs::Blueprint.new(redis: redis_config, concurrency: 1)
    use_real_redis!(@config)
    @config.redis_pool.call("FLUSHDB")
  end

  def teardown
    @config.close
  end

  def test_replayed_task_keeps_the_same_task_id
    # A dedicated channel keeps this test isolated from any Engine that a
    # previous stress test in the same process may still be winding down.
    channel = "at-least-once-#{SecureRandom.hex(4)}"
    task_id = SecureRandom.uuid
    returned = SolidJobs.enqueue(
      "ConcurrentStressJob",
      ["payload"],
      channel: channel,
      id: task_id,
    )
    identity = "#{Socket.gethostname}:999999:dead"
    claimed_key = SolidJobs::Keyspace.claimed(identity, 0)
    raw = @config.redis_pool.call(
      "LMOVE",
      SolidJobs::Keyspace.channel(channel),
      claimed_key,
      "RIGHT",
      "LEFT",
    )
    claimed = JSON.parse(raw)

    assert_equal task_id, returned
    assert_equal task_id, claimed["id"]
    assert_equal 1, SolidJobs.recovery!

    replay = JSON.parse(@config.redis_pool.call("LINDEX", SolidJobs::Keyspace.channel(channel), -1))
    assert_equal task_id, replay["id"]
    assert_equal claimed, replay
  end
end
