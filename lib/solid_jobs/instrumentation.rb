# frozen_string_literal: true

require "json"

module SolidJobs
  module NullInstrumenter
    module_function

    def instrument(_name, _payload = nil)
      yield if block_given?
    end
  end

  module Instrumentation
    EVENTS = {
      enqueued: "job.enqueued",
      reserved: "job.reserved",
      journaled: "job.journaled",
      started: "job.started",
      completed: "job.completed",
      failed: "job.failed",
      retry_scheduled: "job.retry_scheduled",
      acknowledged: "job.acknowledged",
      recovered: "job.recovered",
      dead: "job.dead",
    }.freeze

    module_function

    def emit(config, event, envelope, **attributes)
      envelope = JSON.parse(envelope) if envelope.is_a?(String)
      payload = {
        job_id: envelope["id"],
        queue: envelope["channel"],
        job_class: envelope["task"],
        node_id: config.identity,
      }.merge(attributes).reject { |_key, value| value.nil? }
      config.instrumenter.instrument(EVENTS.fetch(event), payload)
      true
    rescue ExecutionHalt
      raise
    rescue StandardError, ScriptError => error
      begin
        config.logger.warn("SolidJobs telemetry failed: #{error.class}: #{error.message}")
      rescue ExecutionHalt
        raise
      rescue StandardError, ScriptError
        nil
      end
      false
    end

    def worker_attributes(processor_id)
      return {} if processor_id.nil?

      { ractor_id: processor_id, worker_id: processor_id }
    end
  end
end
