# frozen_string_literal: true

module SolidJobs
  module ActiveJob
    class Wrapper
      include SolidJobs::Task

      def perform(job_data)
        ::ActiveJob::Base.execute(job_data.merge("provider_job_id" => task_id))
      end
    end
  end
end
