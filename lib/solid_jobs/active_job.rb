# frozen_string_literal: true

module SolidJobs
  module ActiveJob
    class Wrapper
      include SolidJobs::Job
      solid_jobs_options retry: true

      def perform(job_data)
        ::ActiveJob::Base.execute(job_data.merge("provider_job_id" => jid))
      end
    end
    JobWrapper = Wrapper
  end
end

