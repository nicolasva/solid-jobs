# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __dir__))

require "solid_jobs"

class CrashWorkerJob
  include SolidJobs::Task

  def execute_task
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:crash-started", "1")
    sleep 60
  end
end

class AfterEffectCrashJob
  include SolidJobs::Task

  def execute_task
    SolidJobs.config.redis_pool.pipelined do |pipeline|
      pipeline.call("INCR", "solid-jobs:effect-count")
      pipeline.call("HINCRBY", "solid-jobs:effect-attempts", task_id, 1)
    end
  end
end

class PauseAfterExecution
  def around(_context)
    result = yield
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:after-execution", "1")
    sleep 60
    result
  end
end

redis = SolidRedis::Config.new(url: ARGV.fetch(0), timeout: 0.25, reconnect_attempts: 2)
config = SolidJobs::Blueprint.new(redis: redis, concurrency: Integer(ARGV.fetch(2, "1")))
config.shutdown_timeout = 1
config.execute_interceptors.use(PauseAfterExecution) if ARGV[1] == "after-execution"
SolidJobs.use_config(config)
SolidJobs::Conductor.new(config: config).start
sleep
