# frozen_string_literal: true

require "json"

module SolidJobs
  class Executor
    Execution = Data.define(:task, :envelope)

    attr_reader :config

    def initialize(redis_config:, config: nil)
      @config = config || Blueprint.new(redis: redis_config)
    end

    def execute(envelope)
      envelope = JSON.parse(envelope) if envelope.is_a?(String)
      raise InvalidJobError, "Task envelope must be a Hash" unless envelope.is_a?(Hash)

      task_name = envelope["task"]
      raise InvalidJobError, "Task must be present" unless task_name.is_a?(String) && !task_name.empty?
      arguments = envelope["arguments"]
      raise InvalidJobError, "Task arguments must be an Array" unless arguments.is_a?(Array)
      unless envelope["channel"].is_a?(String) && !envelope["channel"].empty?
        raise InvalidJobError, "Task channel must be present"
      end

      task = Utilities.constantize(task_name).new
      task.task_id = envelope["id"] if task.respond_to?(:task_id=)
      interceptors = config.execute_interceptors
      return task.execute_task(*arguments) if interceptors.empty?

      interceptors.call(Execution.new(task, envelope)) { task.execute_task(*arguments) }
    end
  end
end
