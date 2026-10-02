# frozen_string_literal: true

require "json"

module SolidJobs
  class FailurePolicy < Service::Base

    def call
      payload = @payload.dup
      now = Time.now
      now_ms = Utilities.realtime_milliseconds
      failure_count = Integer(payload.fetch("failure_count", 0)) + 1
      payload["exception_message"] = @error.message.to_s
      payload["exception_type"] = @error.class.name
      payload["first_failed_ms"] ||= now_ms
      payload["last_failed_ms"] = now_ms
      payload["failure_count"] = failure_count
      payload["channel"] = payload["retry_channel"] if payload["retry_channel"]

      if retry_allowed?(payload, failure_count, now)
        decision = retry_decision(payload, failure_count)
        return nil if decision == :drop
        return archive(payload, now.to_f) if decision == :archive

        next_attempt = now.to_f + (decision || retry_delay(failure_count))
        @config.redis_pool.call("ZADD", Keyspace::RETRIES, next_attempt, JSON.generate(payload))
        append_message("retry scheduled")
        next_attempt
      else
        invoke_final_failure(payload)
        archive(payload, now.to_f)
        append_message("task discarded")
        nil
      end
    end

    private

    def retry_allowed?(payload, failure_count, now)
      if payload["retry_within"]
        first_failure = Float(payload["first_failed_ms"]) / 1_000
        now.to_f - first_failure < Float(payload["retry_within"])
      else
        maximum = Integer(payload.fetch("max_failures", 0))
        failure_count <= maximum
      end
    end

    def retry_delay(failure_count)
      ceiling = [
        Float(@config.retry_base_delay) * (2**(failure_count - 1)),
        Float(@config.retry_max_delay),
      ].min
      (ceiling / 2) + (rand * ceiling / 2)
    end

    def retry_decision(payload, failure_count)
      task = resolve_task(payload)
      return unless task.respond_to?(:retry_delay_callback)

      value = task.retry_delay_callback(failure_count, @error, payload)
      return value if value.is_a?(Numeric) || %i[drop archive].include?(value)

      nil
    end

    def invoke_final_failure(payload)
      task = resolve_task(payload)
      return unless task.respond_to?(:final_failure_callback)

      task.final_failure_callback(payload, @error)
    end

    def resolve_task(payload)
      Utilities.constantize(payload.fetch("task"))
    rescue NameError
      nil
    end

    def archive(payload, timestamp)
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("ZADD", Keyspace::DISCARDED, timestamp, JSON.generate(payload))
        pipeline.call(
          "ZREMRANGEBYSCORE",
          Keyspace::DISCARDED,
          "-inf",
          timestamp - @config.discarded_retention,
        )
        pipeline.call(
          "ZREMRANGEBYRANK",
          Keyspace::DISCARDED,
          0,
          -(@config.discarded_limit + 1),
        )
      end
    end
  end
end
