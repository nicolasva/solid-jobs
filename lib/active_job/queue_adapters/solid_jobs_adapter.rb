# frozen_string_literal: true

require "solid_jobs"
require "solid_jobs/active_job"

module ActiveJob
  module QueueAdapters
    class SolidJobsAdapter
      def enqueue(job)
        job.provider_job_id = push(job)
      end

      def enqueue_at(job, timestamp)
        job.provider_job_id = push(job, at: timestamp)
      end

      def enqueue_all(jobs)
        jobs.count do |job|
          job.provider_job_id = job.scheduled_at ? enqueue_at(job, job.scheduled_at.to_f) : enqueue(job)
        end
      end

      def stopping?
        false
      end

      private

      def push(job, at: nil)
        envelope = {
          "task" => SolidJobs::ActiveJob::Wrapper,
          "wrapped" => job.class.name,
          "channel" => job.queue_name,
          "arguments" => [job.serialize],
        }
        envelope["run_at"] = at if at
        SolidJobs::Publisher.publish(envelope)
      end
    end
  end
end
