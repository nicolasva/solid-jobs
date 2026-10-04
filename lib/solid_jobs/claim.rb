# frozen_string_literal: true

require "json"
require "securerandom"
require "digest/sha1"

module SolidJobs
  class Claim
    TIMEOUT = 0.25
    RESERVE = <<~LUA.freeze
      local payload = redis.call("rpop", KEYS[1])
      if not payload then
        return nil
      end

      redis.call("lpush", KEYS[2], payload)
      local job = cjson.decode(payload)
      local attempt = redis.call("hincrby", KEYS[4], job["id"], 1)
      local metadata = {
        task_id = job["id"],
        claim_token = ARGV[1],
        node_id = ARGV[2],
        executor_id = ARGV[3],
        channel = job["channel"],
        claimed_at = tonumber(ARGV[4]),
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
      if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
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
      if cjson.decode(metadata)["claim_token"] ~= ARGV[3] then
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

    ClaimRecord = Struct.new(
      :channel,
      :payload,
      :envelope,
      :redis_pool,
      :claimed_key,
      :claims_key,
      :executor_field,
      :claim_token,
      :attempt,
      keyword_init: true,
    ) do
      def complete
        if claimed_key
          removed = begin
            redis_pool.call(
              "EVALSHA", Claim::ACK_SHA, 3,
              claimed_key, claims_key, Keyspace::ATTEMPTS,
              payload, executor_field, claim_token, envelope.fetch("id"),
            )
          rescue SolidRedis::CommandError => error
            raise unless error.message.include?("NOSCRIPT")

            redis_pool.call(
              "EVAL", Claim::ACK, 3,
              claimed_key, claims_key, Keyspace::ATTEMPTS,
              payload, executor_field, claim_token, envelope.fetch("id"),
            )
          end
          return Integer(removed) == 1
        end
        true
      end

      def requeue
        if claimed_key
          destination = Keyspace.channel(channel)
          removed = begin
            redis_pool.call(
              "EVALSHA", Claim::REQUEUE_SHA, 3,
              claimed_key, claims_key, destination,
              payload, executor_field, claim_token,
            )
          rescue SolidRedis::CommandError => error
            raise unless error.message.include?("NOSCRIPT")

            redis_pool.call(
              "EVAL", Claim::REQUEUE, 3,
              claimed_key, claims_key, destination,
              payload, executor_field, claim_token,
            )
          end
          return Integer(removed) == 1
        else
          redis_pool.call("RPUSH", Keyspace.channel(channel), payload)
        end
        true
      end
    end

    def initialize(config, identity: nil, processor_id: nil)
      @redis_pool = config.redis_pool
      @channel_order = config.channel_order
      @reliable = config.reliable_fetch
      @channels = config.channel_entries.flat_map do |name, weight|
        Array.new(weight, Keyspace.channel(name))
      end.freeze
      @channel_names = @channels.to_h do |key|
        [key, key.delete_prefix("#{Keyspace::PREFIX}:channel:")]
      end.freeze
      @available_channels = @channels
      @claimed_key = Keyspace.claimed(identity, processor_id) if @reliable && identity
      @claims_key = Keyspace.claims(identity) if @claimed_key
      @identity = identity
      @processor_id = processor_id
      @executor_field = processor_id.to_s
      @paused = []
      @paused_refresh_at = 0.0
      @channel_index = 0
      @check_claimed = !!@claimed_key
    end

    def next
      if @check_claimed
        @check_claimed = false
        if (payload = @redis_pool.call("LINDEX", @claimed_key, -1))
          return existing_claim(payload)
        end
      end

      refresh_paused
      channels = @available_channels
      return sleep(0.05) if channels.empty?

      if @claimed_key
        claim_reliable(channels)
      else
        channels = channels.shuffle if @channel_order == :shuffle
        result = @redis_pool.blocking_call(TIMEOUT, "BRPOP", *channels, TIMEOUT)
        result && record(
          result.fetch(1),
          result.fetch(0).delete_prefix("#{Keyspace::PREFIX}:channel:"),
        )
      end
    end

    def connection_failed!
      @check_claimed = !!@claimed_key
    end

    private

    def record(payload, channel = nil, claim_token: nil, attempt: nil)
      envelope = JSON.parse(payload)
      channel ||= envelope.fetch("channel")
      claim_token, attempt = register(envelope, channel) unless claim_token
      ClaimRecord.new(
        channel: channel,
        payload: payload,
        envelope: envelope,
        redis_pool: @redis_pool,
        claimed_key: @claimed_key,
        claims_key: @claims_key,
        executor_field: @executor_field,
        claim_token: claim_token,
        attempt: attempt,
      )
    end

    def existing_claim(payload)
      raw = @redis_pool.call("HGET", @claims_key, @processor_id)
      if raw
        metadata = JSON.parse(raw)
        envelope = JSON.parse(payload)
        if metadata["task_id"] == envelope["id"]
          return ClaimRecord.new(
            channel: envelope.fetch("channel"),
            payload: payload,
            envelope: envelope,
            redis_pool: @redis_pool,
            claimed_key: @claimed_key,
            claims_key: @claims_key,
            executor_field: @executor_field,
            claim_token: metadata.fetch("claim_token"),
            attempt: Integer(metadata.fetch("attempt")),
          )
        end
      end
      record(payload)
    end

    def register(envelope, channel)
      return [nil, nil] unless @claimed_key

      claim_token = SecureRandom.hex(16)
      metadata = JSON.generate(
        "task_id" => envelope["id"],
        "claim_token" => claim_token,
        "node_id" => @identity,
        "executor_id" => @processor_id,
        "channel" => channel,
        "claimed_at" => Time.now.to_f,
      )
      attempt = begin
        @redis_pool.call(
          "EVALSHA", REGISTER_SHA, 2,
          @claims_key, Keyspace::ATTEMPTS,
          envelope["id"], @processor_id, metadata,
        )
      rescue SolidRedis::CommandError => error
        raise unless error.message.include?("NOSCRIPT")

        @redis_pool.call(
          "EVAL", REGISTER, 2,
          @claims_key, Keyspace::ATTEMPTS,
          envelope["id"], @processor_id, metadata,
        )
      end
      [claim_token, Integer(attempt)]
    end

    def next_channel(channels)
      return channels.sample if @channel_order == :shuffle

      channel = channels[@channel_index % channels.length]
      @channel_index += 1
      channel
    end

    def claim_reliable(channels)
      if @channel_order == :priority
        channels.each do |source|
          claim = claim_now(source)
          return claim if claim
        end
        source = channels.first
      else
        source = next_channel(channels)
        claim = claim_now(source)
        return claim if claim
      end
      payload = @redis_pool.blocking_call(
        TIMEOUT,
        "BLMOVE", source, @claimed_key, "RIGHT", "LEFT", TIMEOUT,
      )
      payload && record(payload, @channel_names.fetch(source))
    end

    def claim_now(source)
      claim_token = SecureRandom.hex(16)
      claimed_at = Time.now.to_f
      result = begin
        @redis_pool.call(
          "EVALSHA", RESERVE_SHA, 4,
          source, @claimed_key, @claims_key, Keyspace::ATTEMPTS,
          claim_token, @identity, @processor_id, claimed_at,
        )
      rescue SolidRedis::CommandError => error
        raise unless error.message.include?("NOSCRIPT")

        @redis_pool.call(
          "EVAL", RESERVE, 4,
          source, @claimed_key, @claims_key, Keyspace::ATTEMPTS,
          claim_token, @identity, @processor_id, claimed_at,
        )
      end
      return unless result

      payload, attempt = result
      record(
        payload,
        @channel_names.fetch(source),
        claim_token: claim_token,
        attempt: Integer(attempt),
      )
    end

    def refresh_paused
      now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      return if now < @paused_refresh_at

      @paused = Array(@redis_pool.call("SMEMBERS", Keyspace::PAUSED_CHANNELS))
      @available_channels = if @paused.empty?
        @channels
      else
        @channels.reject { |key| @paused.include?(@channel_names.fetch(key)) }
      end
      @paused_refresh_at = now + 5
    end
  end
end
