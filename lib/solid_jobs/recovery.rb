# frozen_string_literal: true

require "json"
require "socket"

module SolidJobs
  class Recovery < Service::Base
    CLAIMED_PATTERN = "#{Keyspace::PREFIX}:node:*:claimed:*".freeze
    RESTORE_ONE = <<~LUA.freeze
      local payload = redis.call("rpop", KEYS[1])
      if payload then
        local envelope = cjson.decode(payload)
        local publication = envelope["#{PublicationBarrier::FIELD}"]
        if not publication then
          publication = ARGV[3]
          envelope["#{PublicationBarrier::FIELD}"] = publication
          payload = cjson.encode(envelope)
        end
        redis.call(
          "set",
          ARGV[1] .. publication,
          ARGV[4],
          "PX",
          ARGV[2]
        )
        redis.call("rpush", "#{Keyspace::PREFIX}:channel:" .. envelope["channel"], payload)
        return {payload, publication, ARGV[4]}
      end
    LUA

    def call
      recovered = 0
      scan_keys.each do |key|
        node_key = key.split(":claimed:", 2).first
        identity = node_key.delete_prefix("#{Keyspace::PREFIX}:node:")
        next if alive?(identity)

        while (restored = @config.redis_pool.call(
          "EVAL", RESTORE_ONE, 1, key,
          Keyspace::PUBLICATION_PREFIX, PublicationBarrier::TIMEOUT_MS,
          SecureRandom.hex(16), SecureRandom.hex(16),
        ))
          payload, publication, owner = restored
          recovered += 1
          Instrumentation.emit(@config, :recovered, payload)
          release_publication(publication, owner)
        end
        @config.redis_pool.call("DEL", key)
        @config.redis_pool.call("DEL", Keyspace.claims(identity))
      end
      recovered
    end

    private

    def release_publication(publication, owner)
      entry = PublicationBarrier.prepare(publication: publication, owner: owner)
      PublicationBarrier.release(@config.redis_pool, [entry])
    rescue SolidRedis::ConnectionError, SolidRedis::CommandError => error
      @config.logger.warn(
        "SolidJobs recovery barrier release failed; waiting for expiry: #{error.class}: #{error.message}",
      )
    end

    def alive?(identity)
      hostname, pid = identity.split(":", 3)
      return false if hostname == Socket.gethostname && !process_alive?(Integer(pid))

      beat = @config.redis_pool.call("HGET", Keyspace.node(identity), "beat")
      beat && Time.now.to_f - Float(beat) < Heartbeat::TTL
    end

    def process_alive?(pid)
      ::Process.kill(0, pid)
      true
    rescue Errno::ESRCH
      false
    rescue Errno::EPERM
      true
    end

    def scan_keys
      cursor = "0"
      keys = []
      loop do
        cursor, found = @config.redis_pool.call(
          "SCAN", cursor, "MATCH", CLAIMED_PATTERN, "COUNT", 100,
        )
        keys.concat(found)
        break if cursor == "0"
      end
      keys
    end
  end
end
