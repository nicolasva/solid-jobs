# frozen_string_literal: true

require "json"

module SolidJobs
  class Worker
    attr_reader :config

    def initialize(redis_config:, config: nil)
      @config = config || Config.new(redis: redis_config)
    end

    def perform(payload)
      payload = JSON.parse(payload) if payload.is_a?(String)
      raise InvalidJobError, "Job payload must be a Hash" unless payload.is_a?(Hash)

      class_name = payload["class"] || payload["job_class"]
      raise InvalidJobError, "Job must include a non-empty class" if class_name.nil? || class_name.empty?
      raise InvalidJobError, "Job args must be an Array" unless payload["args"].is_a?(Array)
      raise InvalidJobError, "Job queue must be present" if payload["queue"].nil? || payload["queue"].empty?

      job_class = Utilities.constantize(class_name)
      job = job_class.new
      job.jid = payload["jid"] if job.respond_to?(:jid=)

      if config.server_middleware.empty?
        job.perform(*payload.fetch("args"))
      else
        config.server_middleware.invoke(job, payload, payload.fetch("queue")) do
          job.perform(*payload.fetch("args"))
        end
      end
    end
  end
end
