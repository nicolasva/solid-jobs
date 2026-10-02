# frozen_string_literal: true

require "json"

module SolidJobs
  class Timer
    COLLECTIONS = [Keyspace::RETRIES, Keyspace::PLANNED].freeze
    POP_DUE = <<~LUA.freeze
      local jobs = redis.call("zrange", KEYS[1], "-inf", ARGV[1], "byscore", "limit", 0, 1)
      if jobs[1] then
        redis.call("zrem", KEYS[1], jobs[1])
        return jobs[1]
      end
    LUA

    def initialize(config)
      @config = config
      @publisher = Publisher.new(config: config)
    end

    def enqueue_due(now = Time.now.to_f)
      count = 0
      COLLECTIONS.each do |collection|
        while (raw = pop_due(collection, now))
          @publisher.publish(JSON.parse(raw))
          count += 1
        end
      end
      count
    end

    def run(stop:)
      until stop.call
        begin
          Recovery.call(config: @config)
          enqueue_due
        rescue SolidRedis::ConnectionError => error
          @config.logger.warn("Redis scheduler poll failed, retrying: #{error.class}: #{error.message}")
        end
        interruptible_sleep(randomized_interval, stop)
      end
    ensure
      @config.close
    end

    private

    def pop_due(collection, now)
      @config.redis_pool.call("EVAL", POP_DUE, 1, collection, now)
    end

    def randomized_interval
      average = Float(@config.poll_interval_average)
      average * (0.5 + rand)
    end

    def interruptible_sleep(duration, stop)
      deadline = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) + duration
      until stop.call || ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) >= deadline
        sleep 0.05
      end
    end
  end
end
