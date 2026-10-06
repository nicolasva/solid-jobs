# frozen_string_literal: true

require "base_service"
require "callback_collection"
require "solid_redis"

require_relative "solid_jobs/version"
require_relative "solid_jobs/errors"
require_relative "solid_jobs/utilities"
require_relative "solid_jobs/keyspace"
require_relative "solid_jobs/ractor_support"
require_relative "solid_jobs/startup_barrier"
require_relative "solid_jobs/interceptor_registry"
require_relative "solid_jobs/instrumentation"
require_relative "solid_jobs/redis_streams_exporter"
require_relative "solid_jobs/blueprint"
require_relative "solid_jobs/publisher"
require_relative "solid_jobs/lab"
require_relative "solid_jobs/task"
require_relative "solid_jobs/executor"
require_relative "solid_jobs/claim"
require_relative "solid_jobs/failure_policy"
require_relative "solid_jobs/timer"
require_relative "solid_jobs/engine"
require_relative "solid_jobs/observations"
require_relative "solid_jobs/heartbeat"
require_relative "solid_jobs/recovery"
require_relative "solid_jobs/integrity_check"
require_relative "solid_jobs/conductor"
require_relative "solid_jobs/catalog"
require_relative "solid_jobs/rails_adapter" if defined?(::ActiveJob::Base)
require_relative "solid_jobs/railtie" if defined?(::Rails::Railtie)

module SolidJobs
  CONFIG_KEY = :solid_jobs_config

  module_function

  # Configuration is Ractor-local: every thread and fiber inside a Ractor
  # shares it, while each Ractor keeps its own isolated configuration.
  def config
    Ractor.current[CONFIG_KEY] ||= Blueprint.new
  end

  def configure
    yield config
    config
  end

  def instrumenter
    config.instrumenter
  end

  def instrumenter=(value)
    config.instrumenter = value
  end

  def use_config(configuration)
    Ractor.current[CONFIG_KEY]&.close
    Ractor.current[CONFIG_KEY] = configuration
  end

  def reset!
    Ractor.current[CONFIG_KEY]&.close
    Ractor.current[CONFIG_KEY] = Blueprint.new
  end

  def testing!(mode, &block)
    Lab.testing!(mode, &block)
  end

  def enqueue(task, arguments, channel: "default", **options)
    Publisher.publish(
      options.merge(
        "task" => task,
        "arguments" => arguments,
        "channel" => channel,
      ),
    )
  end

  def enqueue_many(task, argument_sets, channel: "default", **options)
    Publisher.publish_many(
      options.merge(
        "task" => task,
        "arguments" => argument_sets,
        "channel" => channel,
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

  def redis_streams_exporter
    config.redis_streams_exporter
  end

  def recovery!
    Recovery.call(config: config).result
  end
end
