# frozen_string_literal: true

module SolidJobs
  module Testing
    MODES = %i[disable fake inline].freeze
    STORAGE_KEY = :solid_jobs_testing_jobs
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

    def fake!
      testing!(:fake) { yield } if block_given?
      testing!(:fake) unless block_given?
    end

    def inline!
      testing!(:inline) { yield } if block_given?
      testing!(:inline) unless block_given?
    end

    def disable!
      testing!(:disable) { yield } if block_given?
      testing!(:disable) unless block_given?
    end

    def jobs_for(job_class)
      storage[job_class.name] ||= []
    end

    def clear_all
      storage.clear
    end

    def execute_inline(payload)
      job_class = payload["class"]
      job_class = Utilities.constantize(job_class) if job_class.is_a?(String)
      instance = job_class.new
      instance.jid = payload["jid"] if instance.respond_to?(:jid=)
      SolidJobs.config.server_middleware.invoke(instance, payload, payload.fetch("queue", "default")) do
        instance.perform(*payload.fetch("args"))
      end
    end

    def storage
      Ractor.current[STORAGE_KEY] ||= {}
    end
  end
end
