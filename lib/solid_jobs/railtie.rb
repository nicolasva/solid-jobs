# frozen_string_literal: true

require_relative "rails_adapter"

if defined?(Rails::Railtie)
  module SolidJobs
    class Railtie < Rails::Railtie
      initializer "solid_jobs.active_job" do
        ActiveSupport.on_load(:active_job) do
          require "active_job/queue_adapters/solid_jobs_adapter"
        end
      end
    end
  end
end
