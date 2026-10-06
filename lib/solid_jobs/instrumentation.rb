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

    def observe(config, name, payload)
      config.instrumenter.instrument(name, payload)
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
  end

  class ObservationEmitter
    STOP = :solid_jobs_observation_emitter_stop

    def initialize(config, capacity:)
      @config = config
      @queue = SizedQueue.new(capacity)
      @thread = Thread.new do
        loop do
          message = @queue.pop
          break if message.equal?(STOP)

          Instrumentation.observe(@config, message[0], message[1])
        end
      end
      @thread.report_on_exception = false
    end

    def emit(name, payload)
      @queue.push([name, payload], true)
      true
    rescue ThreadError
      false
    end

    def shutdown(timeout: 0.1)
      @queue.push(STOP, true)
      @thread.join(timeout)
      return unless @thread.alive?

      @thread.kill
      @thread.join
    rescue ThreadError
      @thread.kill
      @thread.join
    end
  end
end
