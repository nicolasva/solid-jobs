# frozen_string_literal: true

require "json"
require "securerandom"
require "digest/sha1"

module SolidJobs
  class Fetch
    TIMEOUT = 0.25
    RESERVE = <<~LUA.freeze
      local payload = redis.call("rpop", KEYS[1])
      if not payload then
        return nil
      end

      redis.call("lpush", KEYS[2], payload)
      local job = cjson.decode(payload)
      local attempt = redis.call("hincrby", KEYS[4], job["jid"], 1)
      local metadata = {
        job_id = job["jid"],
        reservation_id = ARGV[1],
        process_id = ARGV[2],
        worker_id = ARGV[3],
        queue = job["queue"],
        reserved_at = tonumber(ARGV[4]),
        attempt = attempt
      }
      redis.call("hset", KEYS[3], ARGV[3], cjson.encode(metadata))
      return {payload, attempt}
    LUA
    REGISTER = <<~LUA.freeze
      local attempt = redis.call("hincrby", KEYS[2], ARGV[1], 1)
      local metadata = cjson.decode(ARGV[3])
      metadata["attempt"] = attempt
      redis.call("hset", KEYS[1], ARGV[2], cjson.encode(metadata))
      return attempt
    LUA
    ACK = <<~LUA.freeze
      local metadata = redis.call("hget", KEYS[2], ARGV[2])
      if not metadata then
        return 0
      end
      if cjson.decode(metadata)["reservation_id"] ~= ARGV[3] then
        return 0
      end
      local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
      redis.call("hdel", KEYS[2], ARGV[2])
      if removed > 0 then
        redis.call("hdel", KEYS[3], ARGV[4])
      end
      return removed
    LUA
    REQUEUE = <<~LUA.freeze
      local metadata = redis.call("hget", KEYS[2], ARGV[2])
      if not metadata then
        return 0
      end
      if cjson.decode(metadata)["reservation_id"] ~= ARGV[3] then
        return 0
      end
      local removed = redis.call("lrem", KEYS[1], 1, ARGV[1])
      if removed > 0 then
        redis.call("rpush", KEYS[3], ARGV[1])
      end
      redis.call("hdel", KEYS[2], ARGV[2])
      return removed
    LUA
    RESERVE_SHA = Digest::SHA1.hexdigest(RESERVE).freeze
    REGISTER_SHA = Digest::SHA1.hexdigest(REGISTER).freeze
    ACK_SHA = Digest::SHA1.hexdigest(ACK).freeze
    REQUEUE_SHA = Digest::SHA1.hexdigest(REQUEUE).freeze

    UnitOfWork = Struct.new(
      :queue,
      :payload,
      :job,
      :redis_pool,
      :reserved_key,
      :metadata_key,
      :metadata_field,
      :reservation_id,
      :attempt,
      keyword_init: true,
    ) do
      def acknowledge
        if reserved_key
          removed = begin
            redis_pool.call(
              "EVALSHA", Fetch::ACK_SHA, 3,
              reserved_key, metadata_key, "solid-jobs:attempts",
              payload, metadata_field, reservation_id, job.fetch("jid"),
            )
          rescue SolidRedis::CommandError => error
            raise unless error.message.include?("NOSCRIPT")

            redis_pool.call(
              "EVAL", Fetch::ACK, 3,
              reserved_key, metadata_key, "solid-jobs:attempts",
              payload, metadata_field, reservation_id, job.fetch("jid"),
            )
          end
          return Integer(removed) == 1
        end
        true
      end

      def requeue
        if reserved_key
          destination = "queue:#{queue}"
          removed = begin
            redis_pool.call(
              "EVALSHA", Fetch::REQUEUE_SHA, 3,
              reserved_key, metadata_key, destination,
              payload, metadata_field, reservation_id,
            )
          rescue SolidRedis::CommandError => error
            raise unless error.message.include?("NOSCRIPT")

            redis_pool.call(
              "EVAL", Fetch::REQUEUE, 3,
              reserved_key, metadata_key, destination,
              payload, metadata_field, reservation_id,
            )
          end
          return Integer(removed) == 1
        else
          redis_pool.call("RPUSH", "queue:#{queue}", payload)
        end
        true
      end
    end

    def initialize(config, identity: nil, processor_id: nil)
      @redis_pool = config.redis_pool
      @queue_mode = config.queue_mode
      @reliable = config.reliable_fetch
      @queues = config.queue_entries.flat_map do |name, weight|
        Array.new(weight, "queue:#{name}")
      end.freeze
      @reserved_key = "#{identity}:reserved:#{processor_id}" if @reliable && identity
      @metadata_key = "#{identity}:reservations" if @reserved_key
      @identity = identity
      @processor_id = processor_id
      @paused = []
      @paused_refresh_at = 0.0
      @queue_index = 0
      @check_reserved = !!@reserved_key
    end

    def retrieve
      if @check_reserved
        @check_reserved = false
        if (payload = @redis_pool.call("LINDEX", @reserved_key, -1))
          return existing_work(payload)
        end
      end

      refresh_paused
      queues = @queues.reject { |queue| @paused.include?(queue.delete_prefix("queue:")) }
      return sleep(0.05) if queues.empty?

      if @reserved_key
        reserve_reliable(queues)
      else
        queues = queues.shuffle if @queue_mode == :random
        result = @redis_pool.blocking_call(TIMEOUT, "BRPOP", *queues, TIMEOUT)
        result && work(result.fetch(1), result.fetch(0).delete_prefix("queue:"))
      end
    end

    def connection_failed!
      @check_reserved = !!@reserved_key
    end

    private

    def work(payload, queue = nil, reservation_id: nil, attempt: nil)
      job = JSON.parse(payload)
      queue ||= job.fetch("queue")
      reservation_id, attempt = register(job, queue) unless reservation_id
      UnitOfWork.new(
        queue: queue,
        payload: payload,
        job: job,
        redis_pool: @redis_pool,
        reserved_key: @reserved_key,
        metadata_key: @metadata_key,
        metadata_field: @processor_id.to_s,
        reservation_id: reservation_id,
        attempt: attempt,
      )
    end

    def existing_work(payload)
      raw = @redis_pool.call("HGET", @metadata_key, @processor_id)
      if raw
        metadata = JSON.parse(raw)
        job = JSON.parse(payload)
        if metadata["job_id"] == job["jid"]
          return UnitOfWork.new(
            queue: job.fetch("queue"),
            payload: payload,
            job: job,
            redis_pool: @redis_pool,
            reserved_key: @reserved_key,
            metadata_key: @metadata_key,
            metadata_field: @processor_id.to_s,
            reservation_id: metadata.fetch("reservation_id"),
            attempt: Integer(metadata.fetch("attempt")),
          )
        end
      end
      work(payload)
    end

    def register(job, queue)
      return [nil, nil] unless @reserved_key

      reservation_id = SecureRandom.hex(12)
      metadata = JSON.generate(
        "job_id" => job["jid"],
        "reservation_id" => reservation_id,
        "process_id" => @identity,
        "worker_id" => @processor_id,
        "queue" => queue,
        "reserved_at" => Time.now.to_f,
      )
      attempt = begin
        @redis_pool.call(
          "EVALSHA", REGISTER_SHA, 2,
          @metadata_key, "solid-jobs:attempts",
          job["jid"], @processor_id, metadata,
        )
      rescue SolidRedis::CommandError => error
        raise unless error.message.include?("NOSCRIPT")

        @redis_pool.call(
          "EVAL", REGISTER, 2,
          @metadata_key, "solid-jobs:attempts",
          job["jid"], @processor_id, metadata,
        )
      end
      [reservation_id, Integer(attempt)]
    end

    def next_queue(queues)
      return queues.sample if @queue_mode == :random

      queue = queues[@queue_index % queues.length]
      @queue_index += 1
      queue
    end

    def reserve_reliable(queues)
      if @queue_mode == :strict
        queues.each do |source|
          unit = reserve_now(source)
          return unit if unit
        end
        source = queues.first
      else
        source = next_queue(queues)
        unit = reserve_now(source)
        return unit if unit
      end
      payload = @redis_pool.blocking_call(
        TIMEOUT,
        "BLMOVE", source, @reserved_key, "RIGHT", "LEFT", TIMEOUT,
      )
      payload && work(payload, source.delete_prefix("queue:"))
    end

    def reserve_now(source)
      reservation_id = SecureRandom.hex(12)
      reserved_at = Time.now.to_f
      result = begin
        @redis_pool.call(
          "EVALSHA", RESERVE_SHA, 4,
          source, @reserved_key, @metadata_key, "solid-jobs:attempts",
          reservation_id, @identity, @processor_id, reserved_at,
        )
      rescue SolidRedis::CommandError => error
        raise unless error.message.include?("NOSCRIPT")

        @redis_pool.call(
          "EVAL", RESERVE, 4,
          source, @reserved_key, @metadata_key, "solid-jobs:attempts",
          reservation_id, @identity, @processor_id, reserved_at,
        )
      end
      return unless result

      payload, attempt = result
      work(
        payload,
        source.delete_prefix("queue:"),
        reservation_id: reservation_id,
        attempt: Integer(attempt),
      )
    end

    def refresh_paused
      now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      return if now < @paused_refresh_at

      @paused = Array(@redis_pool.call("SMEMBERS", "paused"))
      @paused_refresh_at = now + 5
    end
  end
end
