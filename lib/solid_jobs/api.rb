# frozen_string_literal: true

require "json"

module SolidJobs
  class Stats
    attr_reader :config

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def processed
      integer("stat:processed")
    end

    def failed
      integer("stat:failed")
    end

    def enqueued
      queues.sum(&:size)
    end

    def scheduled_size
      integer_command("ZCARD", "schedule")
    end

    def retry_size
      integer_command("ZCARD", "retry")
    end

    def dead_size
      integer_command("ZCARD", "dead")
    end

    def processes_size
      integer_command("SCARD", "processes")
    end

    def workers_size
      ProcessSet.new(config: config).sum(&:busy)
    end

    def reset
      config.redis_pool.call("DEL", "stat:processed", "stat:failed")
      self
    end

    private

    def queues
      Queue.all(config: config)
    end

    def integer(key)
      Integer(config.redis_pool.call("GET", key) || 0)
    end

    def integer_command(*command)
      Integer(config.redis_pool.call(*command))
    end
  end

  class Queue
    include Enumerable

    attr_reader :name

    def self.all(config: SolidJobs.config)
      Array(config.redis_pool.call("SMEMBERS", "queues")).sort.map do |name|
        new(name, config: config)
      end
    end

    def initialize(name, config: SolidJobs.config)
      @name = String(name)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      Array(@config.redis_pool.call("LRANGE", key, 0, -1)).reverse_each do |raw|
        yield JobRecord.new(raw, config: @config, container: self)
      end
    end

    def size
      Integer(@config.redis_pool.call("LLEN", key))
    end
    alias_method :length, :size

    def latency
      raw = @config.redis_pool.call("LINDEX", key, -1)
      return 0.0 unless raw

      payload = JSON.parse(raw)
      enqueued_at = payload["enqueued_at"] || payload["created_at"]
      return 0.0 unless enqueued_at

      [Time.now.to_f - Float(enqueued_at) / 1_000, 0.0].max
    end

    def clear
      @config.redis_pool.call("DEL", key)
      self
    end

    def delete
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("DEL", key)
        pipeline.call("SREM", "queues", name)
      end
      self
    end

    def paused?
      Array(@config.redis_pool.call("SISMEMBER", "paused", name)).first.to_i == 1
    end

    def pause
      @config.redis_pool.call("SADD", "paused", name)
      self
    end

    def unpause
      @config.redis_pool.call("SREM", "paused", name)
      self
    end

    private

    def key
      "queue:#{name}"
    end
  end

  class JobRecord
    attr_reader :item, :score

    def initialize(raw, score: nil, config: SolidJobs.config, container: nil)
      @raw = raw
      @item = JSON.parse(raw)
      @score = Float(score) if score
      @config = config
      @container = container
    end

    def jid
      item["jid"]
    end

    def queue
      item["queue"]
    end

    def klass
      item["class"]
    end

    def args
      item["args"]
    end

    def created_at
      milliseconds_time(item["created_at"])
    end

    def enqueued_at
      milliseconds_time(item["enqueued_at"])
    end

    def at
      Time.at(score) if score
    end

    def delete
      case @container
      when SortedSet
        @config.redis_pool.call("ZREM", @container.name, @raw)
      when Queue
        @config.redis_pool.call("LREM", "queue:#{@container.name}", 1, @raw)
      else
        false
      end
    end

    def retry
      delete
      Client.new(config: @config).push(item)
    end

    private

    def milliseconds_time(value)
      Time.at(Float(value) / 1_000) if value
    end
  end

  class SortedSet
    include Enumerable

    attr_reader :name

    def initialize(name, config: SolidJobs.config)
      @name = name
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      values = Array(@config.redis_pool.call("ZRANGE", name, 0, -1, "WITHSCORES"))
      values.each_slice(2) do |raw, score|
        yield JobRecord.new(raw, score: score, config: @config, container: self)
      end
    end

    def size
      Integer(@config.redis_pool.call("ZCARD", name))
    end
    alias_method :length, :size

    def clear
      @config.redis_pool.call("DEL", name)
      self
    end

    def fetch(*jids)
      wanted = jids.flatten
      select { |job| wanted.include?(job.jid) }
    end
  end

  class ScheduledSet < SortedSet
    def initialize(config: SolidJobs.config)
      super("schedule", config: config)
    end
  end

  class RetrySet < SortedSet
    def initialize(config: SolidJobs.config)
      super("retry", config: config)
    end

    def retry_all
      each.to_a.each(&:retry)
    end
  end

  class DeadSet < SortedSet
    def initialize(config: SolidJobs.config)
      super("dead", config: config)
    end

    def retry_all
      each.to_a.each(&:retry)
    end
  end

  class Process
    attr_reader :identity

    def initialize(identity, config: SolidJobs.config)
      @identity = identity
      @config = config
      fields = Array(@config.redis_pool.call("HGETALL", identity))
      @attributes = fields.each_slice(2).to_h
      @info = JSON.parse(@attributes.fetch("info", "{}"))
    end

    def hostname
      @info["hostname"]
    end

    def pid
      Integer(@info["pid"] || 0)
    end

    def queues
      @info["queues"] || []
    end

    def busy
      Integer(@attributes["busy"] || 0)
    end

    def concurrency
      Integer(@attributes["concurrency"] || 0)
    end

    def quiet?
      @attributes["quiet"] == "true"
    end

    def beat
      Time.at(Float(@attributes["beat"])) if @attributes["beat"]
    end

    def quiet!
      signal("TSTP")
    end

    def stop!
      signal("TERM")
    end

    def dump_threads
      signal("TTIN")
    end

    def workers
      fields = Array(@config.redis_pool.call("HGETALL", "#{identity}:work"))
      fields.each_slice(2).map do |processor_id, raw|
        Work.new(identity, processor_id, raw)
      end
    end

    private

    def signal(name)
      @config.redis_pool.call("LPUSH", "#{identity}-signals", name)
      true
    end
  end

  class ProcessSet
    include Enumerable

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      Array(@config.redis_pool.call("SMEMBERS", "processes")).each do |identity|
        process = Process.new(identity, config: @config)
        if process.beat
          yield process
        else
          @config.redis_pool.call("SREM", "processes", identity)
        end
      end
    end

    def size
      count
    end
  end

  class Work
    attr_reader :process_id, :processor_id

    def initialize(process_id, processor_id, raw)
      @process_id = process_id
      @processor_id = processor_id
      @attributes = JSON.parse(raw)
    end

    def queue
      @attributes["queue"]
    end

    def payload
      @attributes["payload"]
    end

    def run_at
      Time.at(Float(@attributes["run_at"]))
    end

    def reservation_id
      @attributes["reservation_id"]
    end

    def attempt
      Integer(@attributes["attempt"] || 1)
    end
  end

  class Reservation
    attr_reader :attributes

    def initialize(raw)
      @attributes = JSON.parse(raw)
    end

    %w[job_id reservation_id process_id worker_id queue].each do |name|
      define_method(name) { attributes[name] }
    end

    def attempt
      Integer(attributes["attempt"])
    end

    def reserved_at
      Time.at(Float(attributes["reserved_at"]))
    end
  end

  class ReservationSet
    include Enumerable

    def initialize(config: SolidJobs.config)
      @config = config
    end

    def each
      return enum_for(:each) unless block_given?

      keys.each do |key|
        Array(@config.redis_pool.call("HVALS", key)).each do |raw|
          yield Reservation.new(raw)
        end
      end
    end

    def find_job(job_id)
      find { |reservation| reservation.job_id == job_id }
    end

    private

    def keys
      cursor = "0"
      found = []
      loop do
        cursor, batch = @config.redis_pool.call(
          "SCAN", cursor, "MATCH", "*:reservations", "COUNT", 100,
        )
        found.concat(batch)
        break if cursor == "0"
      end
      found
    end
  end

  class Workers
    include Enumerable

    def initialize(config: SolidJobs.config)
      @processes = ProcessSet.new(config: config)
    end

    def each
      return enum_for(:each) unless block_given?

      @processes.each { |process| process.workers.each { |work| yield work } }
    end

    def size
      count
    end
  end
end
