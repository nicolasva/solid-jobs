# frozen_string_literal: true

require "base_service"
require "callback_collection"
require "solid_redis"

require_relative "solid_jobs/version"
require_relative "solid_jobs/errors"
require_relative "solid_jobs/utilities"
require_relative "solid_jobs/ractor_support"
require_relative "solid_jobs/startup_barrier"
require_relative "solid_jobs/middleware/chain"
require_relative "solid_jobs/config"
require_relative "solid_jobs/client"
require_relative "solid_jobs/testing"
require_relative "solid_jobs/job"
require_relative "solid_jobs/worker"
require_relative "solid_jobs/fetch"
require_relative "solid_jobs/retry_service"
require_relative "solid_jobs/scheduler"
require_relative "solid_jobs/processor"
require_relative "solid_jobs/heartbeat"
require_relative "solid_jobs/recovery"
require_relative "solid_jobs/integrity_check"
require_relative "solid_jobs/server"
require_relative "solid_jobs/api"
require_relative "solid_jobs/active_job" if defined?(::ActiveJob::Base)
require_relative "solid_jobs/rails" if defined?(::Rails::Railtie)

module SolidJobs
  CONFIG_KEY = :solid_jobs_config

  module_function

  # Configuration is Ractor-local: every thread and fiber inside a Ractor
  # shares it, while each Ractor keeps its own isolated configuration.
  def config
    Ractor.current[CONFIG_KEY] ||= Config.new
  end

  def configure
    yield config
    config
  end

  def use_config(configuration)
    Ractor.current[CONFIG_KEY]&.close
    Ractor.current[CONFIG_KEY] = configuration
  end

  def reset!
    Ractor.current[CONFIG_KEY]&.close
    Ractor.current[CONFIG_KEY] = Config.new
  end

  def testing!(mode, &block)
    Testing.testing!(mode, &block)
  end

  def enqueue(job_class, args, queue: "default", **options)
    Client.push(
      options.merge(
        "class" => job_class,
        "args" => args,
        "queue" => queue,
      ),
    )
  end

  def enqueue_bulk(job_class, arguments, queue: "default", **options)
    Client.push_bulk(
      options.merge(
        "class" => job_class,
        "args" => arguments,
        "queue" => queue,
      ),
    )
  end

  def redis(&block)
    raise ArgumentError, "SolidJobs.redis requires a block" unless block

    config.redis(&block)
  end

  def redis_pool
    config.redis_pool
  end

  def recovery!
    Recovery.call(config: config).result
  end
end
