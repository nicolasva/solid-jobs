# frozen_string_literal: true

require "json"
require "socket"

module SolidJobs
  class Recovery < Service::Base
    RESERVED_PATTERN = "*:reserved:*"
    RESTORE_ONE = <<~LUA.freeze
      local payload = redis.call("rpop", KEYS[1])
      if payload then
        local job = cjson.decode(payload)
        redis.call("rpush", "queue:" .. job["queue"], payload)
        return payload
      end
    LUA

    def call
      recovered = 0
      scan_keys.each do |key|
        identity = key.split(":reserved:", 2).first
        next if alive?(identity)

        while @config.redis_pool.call("EVAL", RESTORE_ONE, 1, key)
          recovered += 1
        end
        @config.redis_pool.call("DEL", key)
        @config.redis_pool.call("DEL", "#{identity}:reservations")
      end
      recovered
    end

    private

    def alive?(identity)
      hostname, pid = identity.split(":", 3)
      return false if hostname == Socket.gethostname && !process_alive?(Integer(pid))

      beat = @config.redis_pool.call("HGET", identity, "beat")
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
          "SCAN", cursor, "MATCH", RESERVED_PATTERN, "COUNT", 100,
        )
        keys.concat(found)
        break if cursor == "0"
      end
      keys
    end
  end
end
