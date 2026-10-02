# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __dir__))

require "solid_jobs"

class TortureAccountingJob
  include SolidJobs::Task
  task_options retry: false

  def perform(identifier)
    SolidJobs.redis do |redis|
      redis.pipelined do |pipeline|
        pipeline.call("HINCRBY", "torture:attempts", task_id, 1)
        pipeline.call("SADD", "torture:completed", task_id)
      end
    end
    GC.start if identifier % 1_000 == 0
    sleep 0.0005
  end
end

redis = SolidRedis::Config.new(url: ARGV.fetch(0), timeout: 0.2, reconnect_attempts: 2)
config = SolidJobs::Config.new(redis: redis, concurrency: Integer(ARGV.fetch(1, "4")))
config.shutdown_timeout = 1
SolidJobs.use_config(config)
server = SolidJobs::Server.new(config: config).start
sleep

