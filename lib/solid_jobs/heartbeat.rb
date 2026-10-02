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
        "channels" => @config.channels,
        "labels" => [],
        "identity" => @identity,
      }
      node_key = Keyspace.node(@identity)
      work_key = Keyspace.node_work(@identity)
      busy = @config.redis_pool.call("HLEN", work_key)
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("INCRBY", Keyspace::PROCESSED, processed) if processed.positive?
        pipeline.call("INCRBY", Keyspace::FAILED, failed) if failed.positive?
        pipeline.call("SADD", Keyspace::NODES, @identity)
        pipeline.call(
          "HSET",
          node_key,
          "info", JSON.generate(info),
          "beat", now,
          "busy", busy,
          "quiet", quiet ? "true" : "false",
          "concurrency", @concurrency,
        )
        pipeline.call("EXPIRE", node_key, TTL)
        pipeline.call("DEL", work_key)
        pipeline.call("HSET", work_key, *work.flatten(1)) unless work.empty?
        pipeline.call("EXPIRE", work_key, TTL)
      end
      now
    end

    def cleanup
      @config.redis_pool.pipelined do |pipeline|
        pipeline.call("SREM", Keyspace::NODES, @identity)
        pipeline.call("DEL", Keyspace.node(@identity))
        pipeline.call("DEL", Keyspace.node_work(@identity))
      end
    ensure
      @config.close
    end
  end
end
