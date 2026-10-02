# frozen_string_literal: true

module SolidJobs
  module RailsAdapter
    class AdapterTask
      include SolidJobs::Task

      def execute_task(job_data)
        ::ActiveJob::Base.execute(job_data.merge("provider_job_id" => task_id))
      end
    end
  end
end
