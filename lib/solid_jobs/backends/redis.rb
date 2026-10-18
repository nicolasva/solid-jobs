# frozen_string_literal: true

require "json"

module SolidJobs
  module Backends
    # SolidRedis adapter: the original Redis-backed storage, behind the
    # backend contract. Publishing goes through the same pipeline as before.
    class Redis < Base
      CAPABILITIES = %i[scheduling crash_recovery deduplication].freeze

      attr_reader :config

      # Pass `url:` through the backend spec to target a different Redis
      # server than the default configuration.
      def initialize(config:, owned: false)
        super()
        @config = config
        @owned = owned
        @claimers = {}
      end

      def capabilities = CAPABILITIES

      def enqueue(envelope, **_options)
        envelope = Utilities.stringify_hash_keys(envelope)
        envelope["id"] ||= SecureRandom.uuid
        enqueue_many([envelope])
        { id: envelope["id"], duplicate: false }
      end

      def schedule(envelope, at:, **options)
        enqueue(Utilities.stringify_hash_keys(envelope).merge("run_at" => Float(at)), **options)
      end

      def enqueue_many(envelopes)
        publisher = Publisher.new(config: @config)
        barriers = publisher.persist_redis(envelopes)
        PublicationBarrier.release(@config.redis_pool, barriers)
        []
      end

      def claim(channel)
        claimer(channel).next
      end

      def ack(claim)
        claim.complete
      end

      def retry_job(claim, delay: nil)
        delay ||= 0
        payload = claim.envelope.merge("channel" => claim.channel)
        @config.redis_pool.call("ZADD", Keyspace::RETRIES, Time.now.to_f + delay, JSON.generate(payload))
        claim.complete
      end

      def recover(**)
        Recovery.call(config: @config)
      end

      def close
        @claimers.each_value { |(cfg, _)| cfg.close }
        @claimers.clear
        @config.close if @owned
        self
      end

      private

      def claimer(channel)
        @claimers[channel.to_s] ||= begin
          scoped = Blueprint.new(redis: @config.redis_config, concurrency: 1, channels: [channel.to_s])
          [scoped, Claim.new(scoped, identity: @config.identity, processor_id: "backend-#{channel}")]
        end
        @claimers[channel.to_s].last
      end
    end
  end
end
