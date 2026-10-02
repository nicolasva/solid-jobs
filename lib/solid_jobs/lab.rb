# frozen_string_literal: true

require "securerandom"

module SolidJobs
  module Lab
    MODES = %i[disable capture execute].freeze
    STORAGE_KEY = :solid_jobs_testing_captured
    MODE_KEY = :solid_jobs_testing_mode

    module_function

    def mode
      Ractor.current[MODE_KEY] || :disable
    end

    def testing!(new_mode)
      new_mode = new_mode.to_sym
      raise ArgumentError, "Unknown testing mode: #{new_mode.inspect}" unless MODES.include?(new_mode)

      if block_given?
        previous = mode
        Ractor.current[MODE_KEY] = new_mode
        begin
          yield
        ensure
          Ractor.current[MODE_KEY] = previous
        end
      else
        Ractor.current[MODE_KEY] = new_mode
      end
    end

    def capture!
      testing!(:capture) { yield } if block_given?
      testing!(:capture) unless block_given?
    end

    def execute!
      testing!(:execute) { yield } if block_given?
      testing!(:execute) unless block_given?
    end

    def disable!
      testing!(:disable) { yield } if block_given?
      testing!(:disable) unless block_given?
    end

    def captured_for(task)
      storage[task.name] ||= []
    end

    def clear_all
      storage.clear
    end

    def execute(envelope)
      normalized = Utilities.stringify_hash_keys(envelope)
      normalized["task"] = normalized.fetch("task").name if normalized["task"].is_a?(Class)
      normalized["channel"] ||= "default"
      normalized["id"] ||= SecureRandom.uuid
      Executor.new(redis_config: SolidJobs.config.redis_config).execute(normalized)
    end

    def storage
      Ractor.current[STORAGE_KEY] ||= {}
    end
  end
end
