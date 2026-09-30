# frozen_string_literal: true

require "json"
require "socket"

module SolidJobs
  class Heartbeat
    INTERVAL = 5
    TTL = 60

    def initialize(config, identity:, started_at:, concurrency:)
      @config = config
      @identity = identity
      @started_at = started_at
      @concurrency = concurrency
    end

    def beat(quiet: false, work: {}, processed: 0, failed: 0)
      now = Time.now.to_f
      info = {
        "hostname" => Socket.gethostname,
        "pid" => ::Process.pid,
        "started_at" => @started_at,
        "queues" => @config.queues,
        "labels" => [],
        "identity" => @identity,
      }
      busy = @config.redis_pool.call("HLEN", "#{@identity}:work")
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("INCRBY", "stat:processed", processed) if processed.positive?
        pipeline.call("INCRBY", "stat:failed", failed) if failed.positive?
        pipeline.call("SADD", "processes", @identity)
        pipeline.call(
          "HSET",
          @identity,
          "info", JSON.generate(info),
          "beat", now,
          "busy", busy,
          "quiet", quiet ? "true" : "false",
          "concurrency", @concurrency,
        )
        pipeline.call("EXPIRE", @identity, TTL)
        pipeline.call("DEL", "#{@identity}:work")
        pipeline.call("HSET", "#{@identity}:work", *work.flatten(1)) unless work.empty?
        pipeline.call("EXPIRE", "#{@identity}:work", TTL)
      end
      now
    end

    def cleanup
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("SREM", "processes", @identity)
        pipeline.call("DEL", @identity)
        pipeline.call("DEL", "#{@identity}:work")
      end
    ensure
      @config.close
    end
  end
end
