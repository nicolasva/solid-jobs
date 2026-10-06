# frozen_string_literal: true

require "json"
require "socket"

module SolidJobs
  class Heartbeat
    INTERVAL = 5
    TTL = 60

    def initialize(config, identity:, started_at:, concurrency:, observations: Observations.new)
      @config = config
      @identity = identity
      @started_at = started_at
      @concurrency = concurrency
      @observations = observations
    end

    def beat(quiet: false, work: {}, processed: 0, failed: 0, redis: {}, phase: nil)
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
      observe(quiet: quiet, work: work, redis: redis, phase: phase, observed_at: now)
      now
    end

    def observe(quiet: false, work: {}, redis: {}, phase: nil, observed_at: Time.now.to_f)
      Instrumentation.observe(
        @config,
        "process.observed",
        @observations.process(node_id: @identity, observed_at: observed_at),
      )
      @observations.ractors(
        node_id: @identity,
        concurrency: @concurrency,
        work: work,
        quiet: quiet,
        phase: phase,
        observed_at: observed_at,
      ).each { |payload| Instrumentation.observe(@config, "ractor.observed", payload) }
      @observations.redis(
        node_id: @identity,
        concurrency: @concurrency,
        statuses: redis,
        observed_at: observed_at,
      ).each { |payload| Instrumentation.observe(@config, "redis.observed", payload) }
      true
    end

    def observe_redis(processor_id, status, observed_at: Time.now.to_f)
      payload = @observations.redis_worker(
        node_id: @identity,
        processor_id: processor_id,
        status: status,
        observed_at: observed_at,
      )
      Instrumentation.observe(@config, "redis.observed", payload)
    end

    def observe_ractor(
      processor_id, work, job_id: nil, quiet: false, phase: nil,
      observed_at: Time.now.to_f
    )
      payload = @observations.ractor_worker(
        node_id: @identity,
        processor_id: processor_id,
        work: work,
        job_id: job_id,
        quiet: quiet,
        phase: phase,
        observed_at: observed_at,
      )
      Instrumentation.observe(@config, "ractor.observed", payload)
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
