# frozen_string_literal: true

require "logger"
require "securerandom"
require "socket"

module SolidJobs
  class Blueprint
    DEFAULT_TASK_OPTIONS = {
      "channel" => "default",
      "max_failures" => 25,
    }.freeze

    attr_accessor :concurrency, :discarded_limit, :discarded_retention, :logger,
      :on_complex_arguments, :poll_interval_average, :shutdown_timeout,
      :reliable_fetch, :retry_base_delay, :retry_max_delay, :instrumenter,
      :backend, :url
    attr_reader :default_task_options, :error_handlers, :execute_interceptors,
      :publish_interceptors, :redis_config, :identity

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
      @identity = "#{Socket.gethostname}:#{::Process.pid}:#{SecureRandom.hex(6)}".freeze
      @instrumenter = NullInstrumenter
      @redis_pool = nil
      @backend = :redis
      @url = nil
      @backends = {}
      @execution_mode = :ractor
      @channel_backends = {}
      @backend_adapters = {}
    end

    REDIS_ADAPTERS = %i[redis solid_redis].freeze

    EXECUTION_MODES = %i[ractor thread].freeze

    attr_reader :backends, :channel_backends, :execution_mode

    # How non-Redis backend workers run: :ractor (parallel, one client per
    # Ractor) or :thread (GVL-bound). Redis channels always use Ractors.
    def execution_mode=(mode)
      mode = mode.to_sym
      raise ArgumentError, "execution_mode must be :ractor or :thread" unless EXECUTION_MODES.include?(mode)

      @execution_mode = mode
    end

    # Named backend definitions: { name => { adapter:, url:, ... } }.
    def backends=(definitions)
      close_adapters
      @backends = definitions.to_h { |name, spec| [name.to_sym, spec.transform_keys(&:to_sym)] }
    end

    # Per-channel overrides of the default backend: { "bulk" => :solid_engine }.
    def channel_backends=(mapping)
      @channel_backends = mapping.to_h { |channel, name| [channel.to_s, name.to_sym] }
    end

    def backend_name_for(channel)
      (channel_backends[channel.to_s] || backend).to_sym
    end

    def redis_backend?(name)
      REDIS_ADAPTERS.include?((backends.dig(name.to_sym, :adapter) || name).to_sym)
    end

    # True when any channel can leave the default Redis path.
    def routed?
      !redis_backend?(backend) || !channel_backends.empty?
    end

    def backend_adapter_for(channel)
      backend_adapter(backend_name_for(channel))
    end

    def backend_adapter(name)
      @backend_adapters[name.to_sym] ||= build_backend(name)
    end

    # Channels served by Redis workers, and those served by other backends.
    def redis_channel_entries
      channel_entries.select { |channel, _| redis_backend?(backend_name_for(channel)) }
    end

    def engine_channel_entries
      channel_entries - redis_channel_entries
    end

    # Adapter for :solid_engine (nil when it is not configured).
    def engine_backend
      name = (backends.keys + [backend, *channel_backends.values]).uniq.find do |candidate|
        !redis_backend?(candidate)
      end
      backend_adapter(name) if name
    end

    # A fresh, unshared adapter instance (one connection per worker thread).
    def build_backend(name)
      name = name.to_sym
      spec = backends[name] || {}
      adapter = (spec[:adapter] || name).to_sym
      case adapter
      when :redis, :solid_redis
        return Backends::Redis.new(config: self) unless spec[:url]

        derived = Blueprint.new(redis: SolidRedis.config(url: spec[:url]), concurrency: concurrency)
        derived.logger = logger
        derived.instrumenter = instrumenter
        Backends::Redis.new(config: derived, owned: true)
      when :solid_engine
        options = spec.except(:adapter)
        options[:url] ||= url || ENV.fetch("SOLID_ENGINE_URL") do
          raise ConfigurationError, "SolidEngine backend #{name.inspect} needs a url"
        end
        Backends::SolidEngine.new(**options)
      else
        raise ConfigurationError, "Unknown backend adapter #{adapter.inspect} for #{name.inspect}"
      end
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
      close_adapters
      self
    end

    def inspect
      "#<#{self.class.name} concurrency=#{concurrency} channels=#{channels.inspect}>"
    end

    def ractor_snapshot
      unless Ractor.shareable?(instrumenter)
        raise ArgumentError, "SolidJobs instrumenter must be Ractor-shareable"
      end

      Utilities.shareable_copy(
        redis_config: redis_config,
        concurrency: concurrency,
        channels: redis_channel_entries,
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
        identity: identity,
        instrumenter: instrumenter,
        backend: backend,
        url: url,
        backends: backends,
        channel_backends: channel_backends,
        execution_mode: execution_mode,
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
      config.instance_variable_set(:@identity, snapshot.fetch(:identity))
      config.instrumenter = snapshot.fetch(:instrumenter)
      config.backend = snapshot.fetch(:backend)
      config.url = snapshot.fetch(:url)
      config.backends = snapshot.fetch(:backends)
      config.channel_backends = snapshot.fetch(:channel_backends)
      config.execution_mode = snapshot.fetch(:execution_mode)
      config
    end

    private

    def close_adapters
      @backend_adapters.each_value(&:close)
      @backend_adapters = {}
    end

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
