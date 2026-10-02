# frozen_string_literal: true

module SolidJobs
  module Task
    def self.included(base)
      base.extend(ClassMethods)
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

    module ClassMethods
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
        enqueue_at(Time.now.to_f + Float(delay), *arguments)
      end

      def enqueue_at(time, *arguments)
        timestamp = time.is_a?(Time) ? time.to_f : Float(time)
        publish("task" => self, "arguments" => arguments, "run_at" => timestamp)
      end

      def execute(*arguments)
        Testing.execute(current_task_options.merge("task" => self, "arguments" => arguments))
      end

      def with_options(options)
        Submission.new(self, options)
      end

      def captured
        Testing.captured_for(self)
      end

      def clear_captured
        captured.clear
      end

      def execute_next
        envelope = captured.shift
        raise EmptyQueueError, "No captured task for #{name}" unless envelope

        Testing.execute(envelope)
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
          enqueue_at(time, *arguments)
        else
          publish(arguments)
        end
      end

      def enqueue_after(delay, *arguments)
        enqueue_at(Time.now.to_f + Float(delay), *arguments)
      end

      def enqueue_at(time, *arguments)
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

  class EmptyQueueError < Error; end
end
