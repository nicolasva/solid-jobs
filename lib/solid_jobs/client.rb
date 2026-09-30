# frozen_string_literal: true

require "json"
require "securerandom"

module SolidJobs
  class Client
    DEFAULT_BATCH_SIZE = 1_000
    SCHEDULED_BATCH_SIZE = 100

    def self.push(item)
      new.push(item)
    end

    def self.push_bulk(item)
      new.push_bulk(item)
    end

    def initialize(config: nil, configuration: nil)
      selected = config || configuration || SolidJobs.config
      @config = selected.is_a?(SolidRedis::Config) ? Config.new(redis: selected) : selected
    end

    def push(item)
      payload = normalize_item(item)
      payload = invoke_middleware(payload)
      return unless payload

      verify_json!(payload)
      raw_push([payload])
      payload["jid"]
    end

    def push_bulk(item)
      raise InvalidJobError, "Job payload must be a Hash" unless item.is_a?(Hash)

      source = Utilities.stringify_hash_keys(item)
      args = source.delete("args")
      raise InvalidJobError, "Job 'args' must be an Array or Enumerable" unless args.respond_to?(:each)

      ats = source.delete("at")
      if ats && !ats.is_a?(Numeric) && !ats.is_a?(Array)
        raise InvalidJobError, "Job 'at' must be a Numeric or an Array"
      end
      if ats.is_a?(Array) && args.respond_to?(:size) && ats.size != args.size
        raise InvalidJobError, "Job 'at' Array must have the same size as 'args'"
      end
      batch_size = Integer(source.delete("batch_size") || (ats ? SCHEDULED_BATCH_SIZE : DEFAULT_BATCH_SIZE))
      raise ArgumentError, "batch_size must be positive" unless batch_size.positive?

      explicit_jid = source.delete("jid")
      if explicit_jid && args.respond_to?(:size) && args.size > 1
        raise InvalidJobError, "Explicit jid is only supported for one bulk job"
      end
      base = normalize_item(source.merge("args" => []))
      base.delete("jid")
      result = []
      args.each_slice(batch_size).with_index do |batch, batch_index|
        payloads = []
        batch.each_with_index do |job_args, index|
          raise InvalidJobError, "Bulk job arguments must be Arrays" unless job_args.is_a?(Array)

          payload = base.merge(
            "args" => job_args,
            "jid" => explicit_jid || SecureRandom.hex(12),
          )
          payload["at"] = ats.is_a?(Array) ? ats[batch_index * batch_size + index] : ats if ats
          normalized = invoke_middleware(payload)
          if normalized
            verify_json!(normalized)
            payloads << normalized
            result << normalized["jid"]
          else
            result << nil
          end
        end
        raw_push(payloads)
      end
      result
    end

    def raw_push(payloads)
      return true if payloads.empty?

      if defined?(Testing)
        case Testing.mode
        when :fake
          payloads.each do |payload|
            Testing.jobs_for(Utilities.constantize(payload["class"])) << payload
          end
          return true
        when :inline
          payloads.each { |payload| Testing.execute_inline(payload) }
          return true
        end
      end

      @config.redis_pool.pipelined do |pipeline|
        if payloads.all? { |payload| payload["at"] }
          payloads.each do |payload|
            score = payload.delete("at")
            pipeline.call("ZADD", "schedule", score, JSON.generate(payload))
          end
        elsif payloads.none? { |payload| payload["at"] }
          push_immediate(pipeline, payloads)
        else
          scheduled, immediate = payloads.partition { |payload| payload["at"] }
          scheduled.each do |payload|
            score = payload.delete("at")
            pipeline.call("ZADD", "schedule", score, JSON.generate(payload))
          end
          push_immediate(pipeline, immediate)
        end
      end
      true
    end

    private

    def normalize_item(item)
      raise InvalidJobError, "Job payload must be a Hash" unless item.is_a?(Hash)

      payload = @config.default_job_options.merge(Utilities.stringify_hash_keys(item))
      payload["class"] ||= payload.delete("job_class")
      payload["jid"] ||= payload.delete("job_id")
      job_class = payload["class"]
      args = payload["args"]
      unless job_class.is_a?(Class) || (job_class.is_a?(String) && !job_class.empty?)
        raise InvalidJobError, "Job must include a non-empty class"
      end
      raise InvalidJobError, "Job args must be an Array" unless args.is_a?(Array)

      payload["class"] = job_class.name if job_class.is_a?(Class)
      payload["queue"] = String(payload.fetch("queue"))
      payload["jid"] ||= SecureRandom.hex(12)
      payload["created_at"] ||= Utilities.realtime_milliseconds
      payload
    end

    def invoke_middleware(payload)
      job_class = payload["class"]
      @config.client_middleware.invoke(job_class, payload, payload["queue"], @config.redis_pool) do
        payload
      end
    end

    def push_immediate(pipeline, payloads)
      return if payloads.empty?

      groups = if payloads.all? { |payload| payload["queue"] == payloads.first["queue"] }
        [[payloads.first["queue"], payloads]]
      else
        payloads.group_by { |payload| payload["queue"] }
      end
      groups.each do |queue, jobs|
        pipeline.call("SADD", "queues", queue)
        now = Utilities.realtime_milliseconds
        encoded = jobs.map do |payload|
          payload["enqueued_at"] = now
          JSON.generate(payload)
        end
        pipeline.call("LPUSH", "queue:#{queue}", *encoded)
      end
    end

    def verify_json!(payload)
      invalid = find_invalid_json_value(payload["args"])
      return unless invalid

      message = "Job arguments must contain only JSON-native values with string hash keys; found #{invalid.inspect}"
      case @config.on_complex_arguments
      when :raise
        raise InvalidArgumentError, message
      when :warn
        @config.logger.warn(message)
      end
    end

    def find_invalid_json_value(value)
      case value
      when nil, true, false, String, Integer, Float
        nil
      when Array
        value.each do |nested|
          invalid = find_invalid_json_value(nested)
          return invalid if invalid
        end
        nil
      when Hash
        value.each do |key, nested|
          return key unless key.is_a?(String)

          invalid = find_invalid_json_value(nested)
          return invalid if invalid
        end
        nil
      else
        value
      end
    end
  end
end
