# frozen_string_literal: true

require "logger"

module SolidJobs
  class Config
    DEFAULT_TASK_OPTIONS = {
      "channel" => "default",
      "max_failures" => 25,
    }.freeze

    attr_accessor :concurrency, :discarded_limit, :discarded_retention, :logger,
      :on_complex_arguments, :poll_interval_average, :shutdown_timeout,
      :reliable_fetch, :retry_base_delay, :retry_max_delay
    attr_reader :default_task_options, :error_handlers, :execute_interceptors,
      :publish_interceptors, :redis_config

    def initialize(redis: nil, concurrency: 5, channels: ["default"])
      @redis_config = redis || SolidRedis.config(url: ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0"))
      @concurrency = Integer(concurrency)
      @channels = normalize_channels(channels)
      @default_task_options = DEFAULT_TASK_OPTIONS
      @publish_interceptors = InterceptorRegistry.new
      @execute_interceptors = InterceptorRegistry.new
      @error_handlers = []
      @lifecycle_callbacks = Hash.new { |hash, event| hash[event] = [] }
      @on_complex_arguments = :raise
      @poll_interval_average = 5.0
      @shutdown_timeout = 25.0
      @channel_order = :weighted
      @reliable_fetch = true
      @retry_base_delay = 15.0
      @retry_max_delay = 3_600.0
      @discarded_limit = 10_000
      @discarded_retention = 180 * 24 * 60 * 60
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

    def channels
      @channels.map(&:first)
    end

    def channels=(values)
      @channels = normalize_channels(values)
    end

    def channel_entries
      @channels.dup
    end

    def channel_order
      @channel_order
    end

    def channel_order=(mode)
      mode = mode.to_sym
      unless %i[priority weighted shuffle].include?(mode)
        raise ArgumentError, "channel_order must be :priority, :weighted, or :shuffle"
      end

      @channel_order = mode
    end

    def prioritized?
      channel_order == :priority
    end

    def prioritized=(value)
      self.channel_order = value ? :priority : :weighted
    end

    def default_task_options=(options)
      @default_task_options = Utilities.shareable_copy(
        DEFAULT_TASK_OPTIONS.merge(Utilities.stringify_keys(options)),
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
      "#<#{self.class.name} concurrency=#{concurrency} channels=#{channels.inspect}>"
    end

    def ractor_snapshot
      Utilities.shareable_copy(
        redis_config: redis_config,
        concurrency: concurrency,
        channels: channel_entries,
        channel_order: channel_order,
        reliable_fetch: reliable_fetch,
        default_task_options: default_task_options,
        on_complex_arguments: on_complex_arguments,
        poll_interval_average: poll_interval_average,
        shutdown_timeout: shutdown_timeout,
        discarded_limit: discarded_limit,
        discarded_retention: discarded_retention,
        retry_base_delay: retry_base_delay,
        retry_max_delay: retry_max_delay,
        publish_interceptors: publish_interceptors.export,
        execute_interceptors: execute_interceptors.export,
      )
    end

    def self.from_ractor_snapshot(snapshot)
      config = new(
        redis: snapshot.fetch(:redis_config),
        concurrency: snapshot.fetch(:concurrency),
        channels: snapshot.fetch(:channels),
      )
      config.channel_order = snapshot.fetch(:channel_order)
      config.reliable_fetch = snapshot.fetch(:reliable_fetch)
      config.default_task_options = snapshot.fetch(:default_task_options)
      config.on_complex_arguments = snapshot.fetch(:on_complex_arguments)
      config.poll_interval_average = snapshot.fetch(:poll_interval_average)
      config.shutdown_timeout = snapshot.fetch(:shutdown_timeout)
      config.discarded_limit = snapshot.fetch(:discarded_limit)
      config.discarded_retention = snapshot.fetch(:discarded_retention)
      config.retry_base_delay = snapshot.fetch(:retry_base_delay)
      config.retry_max_delay = snapshot.fetch(:retry_max_delay)
      config.publish_interceptors.import(snapshot.fetch(:publish_interceptors))
      config.execute_interceptors.import(snapshot.fetch(:execute_interceptors))
      config
    end

    private

    def normalize_channels(values)
      Array(values).map do |value|
        name, weight = value.is_a?(Array) ? value : [value, 1]
        name = String(name)
        weight = Integer(weight)
        raise ArgumentError, "Channel weight must be positive" unless weight.positive?

        [name.freeze, weight].freeze
      end
    end
  end
end
