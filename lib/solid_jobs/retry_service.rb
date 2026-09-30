# frozen_string_literal: true

require "json"

module SolidJobs
  class RetryService < Service::Base
    DEFAULT_MAX_RETRIES = 25

    def call
      payload = @payload.dup
      now = Time.now
      now_ms = Utilities.realtime_milliseconds
      retry_count = payload.key?("retry_count") ? Integer(payload["retry_count"]) + 1 : 0
      payload["error_message"] = @error.message.to_s
      payload["error_class"] = @error.class.name
      payload["failed_at"] ||= now_ms
      payload["retried_at"] = now_ms if retry_count.positive?
      payload["retry_count"] = retry_count
      payload["queue"] = payload["retry_queue"] if payload["retry_queue"]

      setting = payload["retry"]
      if setting && retry_allowed?(payload, setting, retry_count, now)
        decision = retry_decision(payload, retry_count)
        return nil if decision == :discard
        return move_to_dead(payload, now.to_f) if decision == :kill

        retry_at = now.to_f + (decision || retry_delay(retry_count))
        @config.redis_pool.call("ZADD", "retry", retry_at, JSON.generate(payload))
        append_message("retry")
        retry_at
      else
        invoke_exhausted(payload)
        move_to_dead(payload, now.to_f)
        append_message("dead")
        nil
      end
    end

    private

    def retry_allowed?(payload, setting, retry_count, now)
      if payload["retry_for"]
        first_failure = Float(payload["failed_at"]) / 1_000
        now.to_f - first_failure < Float(payload["retry_for"])
      else
        maximum = setting == true ? DEFAULT_MAX_RETRIES : Integer(setting)
        retry_count < maximum
      end
    end

    def retry_delay(retry_count)
      ceiling = [
        Float(@config.retry_base_delay) * (2**retry_count),
        Float(@config.retry_max_delay),
      ].min
      (ceiling / 2) + (rand * ceiling / 2)
    end

    def retry_decision(payload, retry_count)
      job_class = resolve_job_class(payload)
      return unless job_class.respond_to?(:solid_jobs_retry_in_callback)

      value = job_class.solid_jobs_retry_in_callback(retry_count, @error, payload)
      return value if value.is_a?(Numeric) || %i[discard kill].include?(value)

      nil
    end

    def invoke_exhausted(payload)
      job_class = resolve_job_class(payload)
      return unless job_class.respond_to?(:solid_jobs_retries_exhausted_callback)

      job_class.solid_jobs_retries_exhausted_callback(payload, @error)
    end

    def resolve_job_class(payload)
      Utilities.constantize(payload.fetch("class"))
    rescue NameError
      nil
    end

    def move_to_dead(payload, timestamp)
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("ZADD", "dead", timestamp, JSON.generate(payload))
        pipeline.call(
          "ZREMRANGEBYSCORE",
          "dead",
          "-inf",
          timestamp - @config.dead_timeout,
        )
        pipeline.call(
          "ZREMRANGEBYRANK",
          "dead",
          0,
          -(@config.dead_max_jobs + 1),
        )
      end
    end
  end
end
