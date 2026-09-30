# frozen_string_literal: true

require "logger"

module SolidJobs
  class Config
    DEFAULT_JOB_OPTIONS = {
      "queue" => "default",
      "retry" => true,
    }.freeze

    attr_accessor :concurrency, :dead_max_jobs, :dead_timeout, :logger,
      :on_complex_arguments, :poll_interval_average, :shutdown_timeout,
      :reliable_fetch, :retry_base_delay, :retry_max_delay
    attr_reader :client_middleware, :default_job_options, :error_handlers,
      :redis_config, :server_middleware

    def initialize(redis: nil, concurrency: 5, queues: ["default"])
      @redis_config = redis || SolidRedis.config(url: ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0"))
      @concurrency = Integer(concurrency)
      @queues = normalize_queues(queues)
      @default_job_options = DEFAULT_JOB_OPTIONS
      @client_middleware = Middleware::Chain.new
      @server_middleware = Middleware::Chain.new
      @error_handlers = []
      @lifecycle_callbacks = Hash.new { |hash, event| hash[event] = [] }
      @on_complex_arguments = :raise
      @poll_interval_average = 5.0
      @shutdown_timeout = 25.0
      @queue_mode = :weighted
      @reliable_fetch = true
      @retry_base_delay = 15.0
      @retry_max_delay = 3_600.0
      @dead_max_jobs = 10_000
      @dead_timeout = 180 * 24 * 60 * 60
      @logger = Logger.new($stdout)
      @redis_pool = nil
    end

    def redis=(configuration)
      close
      @redis_config = configuration
    end

    def redis_pool
      @redis_pool ||= redis_config.new_pool(size: concurrency + 2, timeout: 1.0)
    end

    def redis
      redis_pool.with { |connection| yield connection }
    end

    def queues
      @queues.map(&:first)
    end

    def queues=(values)
      @queues = normalize_queues(values)
    end

    def queue_entries
      @queues.dup
    end

    def queue_mode
      @queue_mode
    end

    def queue_mode=(mode)
      mode = mode.to_sym
      unless %i[strict weighted random].include?(mode)
        raise ArgumentError, "queue_mode must be :strict, :weighted, or :random"
      end

      @queue_mode = mode
    end

    def strict
      queue_mode == :strict
    end

    def strict=(value)
      self.queue_mode = value ? :strict : :weighted
    end

    def default_job_options=(options)
      @default_job_options = Utilities.shareable_copy(
        DEFAULT_JOB_OPTIONS.merge(Utilities.stringify_keys(options)),
      )
    end

    def on(event, &block)
      raise ArgumentError, "A lifecycle callback block is required" unless block

      @lifecycle_callbacks[event.to_sym] << CallbackCollection.new do |callbacks|
        callbacks.public_send(event, &block)
      end
      self
    end

    def fire(event, *arguments)
      @lifecycle_callbacks[event.to_sym].each do |callbacks|
        callbacks.respond_with(event, *arguments)
      end
    end

    def close
      @redis_pool&.close
      @redis_pool = nil
      self
    end

    def inspect
      "#<#{self.class.name} concurrency=#{concurrency} queues=#{queues.inspect}>"
    end

    def ractor_snapshot
      Utilities.shareable_copy(
        redis_config: redis_config,
        concurrency: concurrency,
        queues: queue_entries,
        queue_mode: queue_mode,
        reliable_fetch: reliable_fetch,
        default_job_options: default_job_options,
        on_complex_arguments: on_complex_arguments,
        poll_interval_average: poll_interval_average,
        shutdown_timeout: shutdown_timeout,
        dead_max_jobs: dead_max_jobs,
        dead_timeout: dead_timeout,
        retry_base_delay: retry_base_delay,
        retry_max_delay: retry_max_delay,
        client_middleware: client_middleware.snapshot,
        server_middleware: server_middleware.snapshot,
      )
    end

    def self.from_ractor_snapshot(snapshot)
      config = new(
        redis: snapshot.fetch(:redis_config),
        concurrency: snapshot.fetch(:concurrency),
        queues: snapshot.fetch(:queues),
      )
      config.queue_mode = snapshot.fetch(:queue_mode)
      config.reliable_fetch = snapshot.fetch(:reliable_fetch)
      config.default_job_options = snapshot.fetch(:default_job_options)
      config.on_complex_arguments = snapshot.fetch(:on_complex_arguments)
      config.poll_interval_average = snapshot.fetch(:poll_interval_average)
      config.shutdown_timeout = snapshot.fetch(:shutdown_timeout)
      config.dead_max_jobs = snapshot.fetch(:dead_max_jobs)
      config.dead_timeout = snapshot.fetch(:dead_timeout)
      config.retry_base_delay = snapshot.fetch(:retry_base_delay)
      config.retry_max_delay = snapshot.fetch(:retry_max_delay)
      config.client_middleware.restore(snapshot.fetch(:client_middleware))
      config.server_middleware.restore(snapshot.fetch(:server_middleware))
      config
    end

    private

    def normalize_queues(values)
      Array(values).map do |value|
        name, weight = value.is_a?(Array) ? value : [value, 1]
        name = String(name)
        weight = Integer(weight)
        raise ArgumentError, "Queue weight must be positive" unless weight.positive?

        [name.freeze, weight].freeze
      end
    end
  end
end
