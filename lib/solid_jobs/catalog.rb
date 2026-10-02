# frozen_string_literal: true

require "json"

module SolidJobs
  class Counters
    attr_reader :config

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def completed
      counter(Keyspace::PROCESSED)
    end

    def failed
      counter(Keyspace::FAILED)
    end

    def ready
      Channel.catalog(config: config).sum(&:size)
    end

    def planned
      cardinality(Keyspace::PLANNED)
    end

    def retrying
      cardinality(Keyspace::RETRIES)
    end

    def discarded
      cardinality(Keyspace::DISCARDED)
    end

    def nodes
      Integer(config.redis_pool.call("SCARD", Keyspace::NODES))
    end

    def executions
      Nodes.new(config: config).sum(&:active_count)
    end

    def reset
      config.redis_pool.call("DEL", Keyspace::PROCESSED, Keyspace::FAILED)
      self
    end

    private

    def counter(key)
      Integer(config.redis_pool.call("GET", key) || 0)
    end

    def cardinality(key)
      Integer(config.redis_pool.call("ZCARD", key))
    end
  end

  class Channel
    include Enumerable

    attr_reader :name

    def self.catalog(config: SolidJobs.config)
      Array(config.redis_pool.call("SMEMBERS", Keyspace::CHANNELS)).sort.map do |name|
        new(name, config: config)
      end
    end

    def initialize(name, config: SolidJobs.config)
      @name = String(name)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      Array(@config.redis_pool.call("LRANGE", storage_key, 0, -1)).reverse_each do |raw|
        yield StoredTask.new(raw, config: @config, owner: self)
      end
    end

    def size
      Integer(@config.redis_pool.call("LLEN", storage_key))
    end

    def waiting_time
      raw = @config.redis_pool.call("LINDEX", storage_key, -1)
      return 0.0 unless raw

      envelope = JSON.parse(raw)
      timestamp = envelope["queued_ms"] || envelope["created_ms"]
      timestamp ? [Time.now.to_f - Float(timestamp) / 1_000, 0.0].max : 0.0
    end

    def purge
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("DEL", storage_key)
        pipeline.call("SREM", Keyspace::CHANNELS, name)
      end
      self
    end

    def paused?
      Integer(@config.redis_pool.call("SISMEMBER", Keyspace::PAUSED_CHANNELS, name)) == 1
    end

    def pause
      @config.redis_pool.call("SADD", Keyspace::PAUSED_CHANNELS, name)
      self
    end

    def resume
      @config.redis_pool.call("SREM", Keyspace::PAUSED_CHANNELS, name)
      self
    end

    def storage_key
      Keyspace.channel(name)
    end
  end

  class StoredTask
    attr_reader :envelope, :score

    def initialize(raw, score: nil, config: SolidJobs.config, owner: nil)
      @raw = raw
      @envelope = JSON.parse(raw)
      @score = Float(score) if score
      @config = config
      @owner = owner
    end

    def id
      envelope["id"]
    end

    def channel
      envelope["channel"]
    end

    def task
      envelope["task"]
    end

    def arguments
      envelope["arguments"]
    end

    def created_at
      millisecond_time(envelope["created_ms"])
    end

    def queued_at
      millisecond_time(envelope["queued_ms"])
    end

    def due_at
      Time.at(score) if score
    end

    def remove
      case @owner
      when TimedTasks
        @config.redis_pool.call("ZREM", @owner.key, @raw)
      when Channel
        @config.redis_pool.call("LREM", @owner.storage_key, 1, @raw)
      else
        false
      end
    end

    def requeue
      remove
      Publisher.new(config: @config).publish(envelope)
    end

    private

    def millisecond_time(value)
      Time.at(Float(value) / 1_000) if value
    end
  end

  class TimedTasks
    include Enumerable

    attr_reader :key

    def initialize(key, config: SolidJobs.config)
      @key = key
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      values = Array(@config.redis_pool.call("ZRANGE", key, 0, -1, "WITHSCORES"))
      values.each_slice(2) do |raw, score|
        yield StoredTask.new(raw, score: score, config: @config, owner: self)
      end
    end

    def size
      Integer(@config.redis_pool.call("ZCARD", key))
    end

    def purge
      @config.redis_pool.call("DEL", key)
      self
    end

    def locate(*ids)
      wanted = ids.flatten
      select { |task| wanted.include?(task.id) }
    end
  end

  class PlannedTasks < TimedTasks
    def initialize(config: SolidJobs.config)
      super(Keyspace::PLANNED, config: config)
    end
  end

  class RetryingTasks < TimedTasks
    def initialize(config: SolidJobs.config)
      super(Keyspace::RETRIES, config: config)
    end

    def requeue_all
      each.to_a.each(&:requeue)
    end
  end

  class DiscardedTasks < TimedTasks
    def initialize(config: SolidJobs.config)
      super(Keyspace::DISCARDED, config: config)
    end

    def requeue_all
      each.to_a.each(&:requeue)
    end
  end

  class Node
    CONTROL_SIGNALS = {
      pause: "TSTP",
      shutdown: "TERM",
      backtraces: "TTIN",
    }.freeze

    attr_reader :identity

    def initialize(identity, config: SolidJobs.config)
      @identity = identity
      @config = config
      fields = Array(@config.redis_pool.call("HGETALL", Keyspace.node(identity)))
      @attributes = fields.each_slice(2).to_h
      @info = JSON.parse(@attributes.fetch("info", "{}"))
    end

    def hostname
      @info["hostname"]
    end

    def pid
      Integer(@info["pid"] || 0)
    end

    def channels
      @info["channels"] || []
    end

    def active_count
      Integer(@attributes["busy"] || 0)
    end

    def concurrency
      Integer(@attributes["concurrency"] || 0)
    end

    def quiet?
      @attributes["quiet"] == "true"
    end

    def last_seen_at
      Time.at(Float(@attributes["beat"])) if @attributes["beat"]
    end

    def request_control(action)
      name = CONTROL_SIGNALS.fetch(action)
      @config.redis_pool.call("LPUSH", Keyspace.node_signals(identity), name)
      true
    end

    def executions
      fields = Array(@config.redis_pool.call("HGETALL", Keyspace.node_work(identity)))
      fields.each_slice(2).map do |executor_id, raw|
        Execution.new(identity, executor_id, raw)
      end
    end
  end

  class Nodes
    include Enumerable

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      Array(@config.redis_pool.call("SMEMBERS", Keyspace::NODES)).each do |identity|
        node = Node.new(identity, config: @config)
        if node.last_seen_at
          yield node
        else
          @config.redis_pool.call("SREM", Keyspace::NODES, identity)
        end
      end
    end
  end

  class Execution
    attr_reader :node_id, :executor_id

    def initialize(node_id, executor_id, raw)
      @node_id = node_id
      @executor_id = executor_id
      @attributes = JSON.parse(raw)
    end

    def channel
      @attributes["channel"]
    end

    def envelope
      @attributes["envelope"]
    end

    def started_at
      Time.at(Float(@attributes["started_at"]))
    end

    def claim_token
      @attributes["claim_token"]
    end

    def attempt
      Integer(@attributes["attempt"] || 1)
    end
  end

  class ClaimInfo
    attr_reader :attributes

    def initialize(raw)
      @attributes = JSON.parse(raw)
    end

    %w[task_id claim_token node_id executor_id channel].each do |name|
      define_method(name) { attributes[name] }
    end

    def attempt
      Integer(attributes["attempt"])
    end

    def claimed_at
      Time.at(Float(attributes["claimed_at"]))
    end
  end

  class Claims
    include Enumerable

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      keys.each do |key|
        Array(@config.redis_pool.call("HVALS", key)).each do |raw|
          yield ClaimInfo.new(raw)
        end
      end
    end

    def find_task(task_id)
      find { |claim| claim.task_id == task_id }
    end

    private

    def keys
      cursor = "0"
      found = []
      loop do
        cursor, batch = @config.redis_pool.call(
          "SCAN", cursor, "MATCH", "#{Keyspace::PREFIX}:node:*:claims", "COUNT", 100,
        )
        found.concat(batch)
        break if cursor == "0"
      end
      found
    end
  end

  class Executions
    include Enumerable

    def initialize(config: SolidJobs.config)
      @nodes = Nodes.new(config: config)
    end

    def each
      return enum_for(:each) unless block_given?

      @nodes.each { |node| node.executions.each { |execution| yield execution } }
    end
  end
end
