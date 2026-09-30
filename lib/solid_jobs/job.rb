# frozen_string_literal: true

module SolidJobs
  module Job
    def self.included(base)
      base.extend(ClassMethods)
      base.include(InstanceMethods)
      base.solid_jobs_options
    end

    module InstanceMethods
      attr_accessor :jid
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
      def solid_jobs_options(options = nil)
        if options
          merged = get_solid_jobs_options.merge(Utilities.stringify_keys(options))
          @solid_jobs_options = Utilities.shareable_copy(merged)
        else
          @solid_jobs_options ||= Utilities.shareable_copy(SolidJobs.config.default_job_options)
        end
      end
      alias_method :sidekiq_options, :solid_jobs_options

      def get_solid_jobs_options
        solid_jobs_options
      end
      alias_method :get_sidekiq_options, :get_solid_jobs_options

      def solid_jobs_retry_in(&block)
        define_singleton_method(:solid_jobs_retry_in_callback, &block) if block
        method(:solid_jobs_retry_in_callback) if respond_to?(:solid_jobs_retry_in_callback)
      end
      alias_method :sidekiq_retry_in, :solid_jobs_retry_in

      def solid_jobs_retries_exhausted(&block)
        define_singleton_method(:solid_jobs_retries_exhausted_callback, &block) if block
        method(:solid_jobs_retries_exhausted_callback) if respond_to?(:solid_jobs_retries_exhausted_callback)
      end
      alias_method :sidekiq_retries_exhausted, :solid_jobs_retries_exhausted

      def perform_async(*args)
        client_push("class" => self, "args" => args)
      end

      def perform_bulk(args, batch_size: Client::DEFAULT_BATCH_SIZE)
        Client.new.push_bulk(
          get_solid_jobs_options.merge(
            "class" => self,
            "args" => args,
            "batch_size" => batch_size,
          ),
        )
      end

      def perform_in(interval, *args)
        timestamp = interval.is_a?(Time) ? interval.to_f : Float(interval)
        timestamp += Time.now.to_f if timestamp < 1_000_000_000
        item = {"class" => self, "args" => args}
        item["at"] = timestamp if timestamp > Time.now.to_f
        client_push(item)
      end

      def perform_at(timestamp, *args)
        perform_in(timestamp, *args)
      end

      def perform_inline(*args)
        payload = get_solid_jobs_options.merge(
          "class" => self,
          "args" => args,
        )
        SolidJobs::Testing.execute_inline(payload)
      end
      alias_method :perform_sync, :perform_inline

      def set(options)
        Setter.new(self, options)
      end

      def client_push(item)
        Client.new.push(get_solid_jobs_options.merge(item))
      end

      def jobs
        Testing.jobs_for(self)
      end

      def clear
        jobs.clear
      end

      def perform_one
        payload = jobs.shift
        raise EmptyQueueError, "No jobs for #{name}" unless payload

        Testing.execute_inline(payload)
      end
    end

    class Setter
      def initialize(job_class, options)
        @job_class = job_class
        @options = Utilities.stringify_keys(options)
      end

      def perform_async(*args)
        if @options.delete("sync")
          @job_class.perform_inline(*args)
        elsif (wait = @options.delete("wait"))
          perform_in(wait, *args)
        elsif (wait_until = @options.delete("wait_until"))
          perform_at(wait_until, *args)
        else
          push(args)
        end
      end

      def perform_in(interval, *args)
        timestamp = interval.is_a?(Time) ? interval.to_f : Float(interval)
        timestamp += Time.now.to_f if timestamp < 1_000_000_000
        push(args, "at" => timestamp)
      end

      def perform_at(timestamp, *args)
        perform_in(timestamp, *args)
      end

      private

      def push(args, extra = {})
        Client.new.push(@job_class.get_solid_jobs_options.merge(@options).merge(
          extra,
          "class" => @job_class,
          "args" => args,
        ))
      end
    end
  end

  class EmptyQueueError < Error; end
end
