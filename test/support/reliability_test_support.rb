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

class ReliabilityResultJob
  include SolidJobs::Task

  def execute_task(key, value)
    SolidJobs.config.redis_pool.call("SET", key, value)
  end
end

class ReliabilityEffectJob
  include SolidJobs::Task

  def execute_task(result_key, effects_key, value)
    SolidJobs.config.redis_pool.pipelined do |pipeline|
      pipeline.call("SET", result_key, value)
      pipeline.call("INCR", effects_key)
    end
  end
end

class ReliabilityRetryJob
  include SolidJobs::Task

  def execute_task(result_key, attempts_key)
    attempt = SolidJobs.config.redis_pool.call("INCR", attempts_key)
    raise ArgumentError, "retry once" if attempt == 1

    SolidJobs.config.redis_pool.call("SET", result_key, "recovered")
  end
end

class ReliabilityTerminalJob
  include SolidJobs::Task

  def execute_task
    raise ArgumentError, "terminal failure"
  end
end

class ReliabilityRecoveryJob
  include SolidJobs::Task

  def execute_task(result_key, effects_key)
    SolidJobs.config.redis_pool.pipelined do |pipeline|
      pipeline.call("SET", result_key, "completed")
      pipeline.call("INCR", effects_key)
    end
  end
end

class ReliabilityBlockingJob
  include SolidJobs::Task

  def execute_task(result_key, arrivals_key, release_key)
    SolidJobs.config.redis_pool.call("INCR", arrivals_key)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
    until SolidJobs.config.redis_pool.call("GET", release_key) == "1"
      raise "reliability barrier timed out" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
    SolidJobs.config.redis_pool.call("SET", result_key, "completed")
  end
end
