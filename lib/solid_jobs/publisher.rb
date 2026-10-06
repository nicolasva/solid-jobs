# frozen_string_literal: true

require "json"
require "securerandom"

module SolidJobs
  class Publisher
    DEFAULT_CHUNK_SIZE = 1_000
    PLANNED_CHUNK_SIZE = 100
    Publication = Data.define(:task, :envelope, :redis_pool)

    def self.publish(envelope)
      new.publish(envelope)
    end

    def self.publish_many(envelope)
      new.publish_many(envelope)
    end

    def initialize(config: nil, configuration: nil)
      selected = config || configuration || SolidJobs.config
      @config = selected.is_a?(SolidRedis::Config) ? Blueprint.new(redis: selected) : selected
    end

    def publish(envelope)
      normalized = normalize(envelope)
      normalized = intercept(normalized)
      return unless normalized

      validate_arguments!(normalized.fetch("arguments"))
      persist([normalized])
      emit_persisted(normalized)
      normalized.fetch("id")
    end

    def publish_many(envelope)
      raise InvalidJobError, "Task envelope must be a Hash" unless envelope.is_a?(Hash)

      source = Utilities.stringify_hash_keys(envelope)
      argument_sets = source.delete("arguments")
      unless argument_sets.respond_to?(:each)
        raise InvalidJobError, "Task 'arguments' must be an Array or Enumerable"
      end

      run_times = source.delete("run_at")
      unless run_times.nil? || run_times.is_a?(Numeric) || run_times.is_a?(Array)
        raise InvalidJobError, "Task 'run_at' must be Numeric or an Array"
      end
      if run_times.is_a?(Array) && argument_sets.respond_to?(:size) && run_times.size != argument_sets.size
        raise InvalidJobError, "Task 'run_at' Array must match the number of argument sets"
      end

      chunk_size = Integer(
        source.delete("chunk_size") || (run_times ? PLANNED_CHUNK_SIZE : DEFAULT_CHUNK_SIZE),
      )
      raise ArgumentError, "chunk_size must be positive" unless chunk_size.positive?

      explicit_id = source.delete("id")
      if explicit_id && argument_sets.respond_to?(:size) && argument_sets.size > 1
        raise InvalidJobError, "An explicit id is only valid for a single task"
      end

      template = normalize(source.merge("arguments" => []))
      template.delete("id")
      ids = []
      argument_sets.each_slice(chunk_size).with_index do |chunk, chunk_index|
        envelopes = chunk.filter_map.with_index do |arguments, index|
          raise InvalidJobError, "Each task's arguments must be an Array" unless arguments.is_a?(Array)

          item = template.merge(
            "arguments" => arguments,
            "id" => explicit_id || SecureRandom.uuid,
          )
          item["run_at"] = run_times.is_a?(Array) ? run_times[chunk_index * chunk_size + index] : run_times if run_times
          intercepted = intercept(item)
          if intercepted
            validate_arguments!(intercepted.fetch("arguments"))
            ids << intercepted.fetch("id")
          else
            ids << nil
          end
          intercepted
        end
        persist(envelopes)
        envelopes.each { |envelope| emit_persisted(envelope) }
      end
      ids
    end

    def persist(envelopes)
      return true if envelopes.empty?

      if defined?(Lab)
        case Lab.mode
        when :capture
          envelopes.each do |envelope|
            Lab.captured_for(Utilities.constantize(envelope.fetch("task"))) << envelope
          end
          return true
        when :execute
          envelopes.each { |envelope| Lab.execute(envelope) }
          return true
        end
      end

      @config.redis_pool.pipelined do |pipeline|
        planned, ready = envelopes.partition { |envelope| envelope["run_at"] }
        planned.each do |envelope|
          score = envelope.delete("run_at")
          pipeline.call("ZADD", Keyspace::PLANNED, score, JSON.generate(envelope))
        end
        persist_ready(pipeline, ready)
      end
      true
    end

    private

    def emit_persisted(envelope)
      Instrumentation.emit(@config, :enqueued, envelope)
      Instrumentation.emit(@config, :journaled, envelope)
    end

    def normalize(envelope)
      raise InvalidJobError, "Task envelope must be a Hash" unless envelope.is_a?(Hash)

      item = @config.default_task_options.merge(Utilities.stringify_hash_keys(envelope))
      task = item["task"]
      arguments = item["arguments"]
      unless task.is_a?(Class) || (task.is_a?(String) && !task.empty?)
        raise InvalidJobError, "Task envelope requires a non-empty task"
      end
      raise InvalidJobError, "Task arguments must be an Array" unless arguments.is_a?(Array)

      item["task"] = task.name if task.is_a?(Class)
      item["channel"] = String(item.fetch("channel"))
      item["id"] ||= SecureRandom.uuid
      item["created_ms"] ||= Utilities.realtime_milliseconds
      item
    end

    def intercept(envelope)
      context = Publication.new(envelope.fetch("task"), envelope, @config.redis_pool)
      @config.publish_interceptors.call(context) { envelope }
    end

    def persist_ready(pipeline, envelopes)
      envelopes.group_by { |envelope| envelope.fetch("channel") }.each do |channel, group|
        pipeline.call("SADD", Keyspace::CHANNELS, channel)
        queued_ms = Utilities.realtime_milliseconds
        encoded = group.map do |envelope|
          envelope["queued_ms"] = queued_ms
          JSON.generate(envelope)
        end
        pipeline.call("LPUSH", Keyspace.channel(channel), *encoded)
      end
    end

    def validate_arguments!(arguments)
      invalid = invalid_json_value(arguments)
      return unless invalid

      message = "Task arguments must contain JSON-native values with string hash keys; found #{invalid.inspect}"
      case @config.on_complex_arguments
      when :raise then raise InvalidArgumentError, message
      when :warn then @config.logger.warn(message)
      end
    end

    def invalid_json_value(value)
      case value
      when nil, true, false, String, Integer, Float
        nil
      when Array
        value.each do |nested|
          invalid = invalid_json_value(nested)
          return invalid if invalid
        end
        nil
      when Hash
        value.each do |key, nested|
          return key unless key.is_a?(String)

          invalid = invalid_json_value(nested)
          return invalid if invalid
        end
        nil
      else
        value
      end
    end
  end
end
