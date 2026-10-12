# frozen_string_literal: true

require "json"

module SolidJobs
  module Task
    # Intercepts `enqueue` and reroutes non Ractor-safe tasks to the warm pool.
    module WarmPoolRouting
      NON_SHAREABLE_TYPES = [Mutex, Thread, Fiber].freeze

      def enqueue(*arguments)
        return super unless requires_warm_pool?(arguments)

        require_relative "../../app/jobs/solid_jobs_generic_executor"
        SolidJobsGenericExecutor.enqueue(
          SolidJobsGenericExecutor.worker_id_for(name),
          name,
          Ractor.make_shareable(JSON.parse(JSON.generate(arguments))),
        )
      end

      def requires_warm_pool?(args)
        return false if equal?(defined?(::SolidJobsGenericExecutor) && ::SolidJobsGenericExecutor)
        return false unless defined?(::Rails)

        !args.all? { |argument| Ractor.shareable?(argument) } && holds_blocking_state?
      end

      private

      def holds_blocking_state?
        instance_variables.any? do |ivar|
          value = instance_variable_get(ivar)
          NON_SHAREABLE_TYPES.any? { |type| value.is_a?(type) } || value.class.name.to_s.include?("Pool")
        end
      end
    end

    def self.included(base)
      base.extend(TaskMethods)
      base.singleton_class.prepend(WarmPoolRouting)
      base.include(InstanceMethods)
      base.task_options
    end

    module InstanceMethods
      attr_accessor :task_id
      attr_writer :_context

      def _context
        @_context ||= {}
      end

      def logger
        SolidJobs.config.logger
      end

      def interrupted?
        _context[:interrupted]&.call || false
      end
    end

    module TaskMethods
      def task_options(options = nil)
        if options
          merged = current_task_options.merge(Utilities.stringify_keys(options))
          @task_options = Utilities.shareable_copy(merged)
        else
          @task_options ||= Utilities.shareable_copy(SolidJobs.config.default_task_options)
        end
      end

      def current_task_options
        task_options
      end

      def retry_delay(&block)
        define_singleton_method(:retry_delay_callback, &block) if block
        method(:retry_delay_callback) if respond_to?(:retry_delay_callback)
      end

      def after_final_failure(&block)
        define_singleton_method(:final_failure_callback, &block) if block
        method(:final_failure_callback) if respond_to?(:final_failure_callback)
      end

      def enqueue(*arguments)
        publish("task" => self, "arguments" => arguments)
      end

      def enqueue_many(argument_sets, chunk_size: Publisher::DEFAULT_CHUNK_SIZE)
        Publisher.new.publish_many(
          current_task_options.merge(
            "task" => self,
            "arguments" => argument_sets,
            "chunk_size" => chunk_size,
          ),
        )
      end

      def enqueue_after(delay, *arguments)
        schedule_at(Time.now.to_f + Float(delay), *arguments)
      end

      def schedule_at(time, *arguments)
        timestamp = time.is_a?(Time) ? time.to_f : Float(time)
        publish("task" => self, "arguments" => arguments, "run_at" => timestamp)
      end

      def execute(*arguments)
        Lab.execute(current_task_options.merge("task" => self, "arguments" => arguments))
      end

      def with_options(options)
        Submission.new(self, options)
      end

      def captured
        Lab.captured_for(self)
      end

      def clear_captured
        captured.clear
      end

      def execute_next
        envelope = captured.shift
        raise NoCapturedTask, "No captured task for #{name}" unless envelope

        Lab.execute(envelope)
      end

      private

      def publish(envelope)
        Publisher.new.publish(current_task_options.merge(envelope))
      end
    end

    class Submission
      def initialize(task, options)
        @task = task
        @options = Utilities.stringify_keys(options)
      end

      def enqueue(*arguments)
        return @task.execute(*arguments) if @options.delete("execute")

        if (delay = @options.delete("delay"))
          enqueue_after(delay, *arguments)
        elsif (time = @options.delete("run_at"))
          schedule_at(time, *arguments)
        else
          publish(arguments)
        end
      end

      def enqueue_after(delay, *arguments)
        schedule_at(Time.now.to_f + Float(delay), *arguments)
      end

      def schedule_at(time, *arguments)
        timestamp = time.is_a?(Time) ? time.to_f : Float(time)
        publish(arguments, "run_at" => timestamp)
      end

      private

      def publish(arguments, extra = {})
        Publisher.new.publish(
          @task.current_task_options.merge(@options).merge(
            extra,
            "task" => @task,
            "arguments" => arguments,
          ),
        )
      end
    end
  end

  class NoCapturedTask < Error; end
end
