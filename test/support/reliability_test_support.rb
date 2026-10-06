# frozen_string_literal: true

class RedisResultJob
  include SolidJobs::Task

  def execute_task(value)
    SolidJobs.config.redis_pool.call("SET", "solid-jobs:test-result", value)
  end
end

class TelemetryFailureJob
  include SolidJobs::Task

  def execute_task
    raise ArgumentError, "expected failure"
  end
end

class TelemetryBlockingJob
  include SolidJobs::Task

  def execute_task
    SolidJobs.config.redis_pool.call("INCR", "solid-jobs:telemetry-blocking")
    sleep 0.2
  end
end

class RedisTelemetryInstrumenter
  KEY = "solid-jobs:test-telemetry"

  def self.instrument(name, payload)
    SolidJobs.config.redis_pool.call("RPUSH", KEY, JSON.generate("name" => name, "payload" => payload))
  end
end
