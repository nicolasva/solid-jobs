# frozen_string_literal: true

require "securerandom"

module SolidJobs
  module PublicationBarrier
    TIMEOUT_MS = 30_000
    FIELD = "_solid_jobs_publication"
    Entry = Data.define(:key, :publication, :owner)
    RELEASE = <<~LUA.freeze
      local released = 0
      for index, key in ipairs(KEYS) do
        if redis.call("get", key) == ARGV[index] then
          released = released + redis.call("del", key)
        end
      end
      return released
    LUA

    module_function

    def prepare(publication: SecureRandom.hex(16), owner: SecureRandom.hex(16))
      Entry.new(Keyspace.publication(publication), publication, owner)
    end

    def mark(target, entry)
      target.call("SET", entry.key, entry.owner, "PX", TIMEOUT_MS)
    end

    def release(redis_pool, entries)
      entries = Array(entries)
      return 0 if entries.empty?

      redis_pool.call(
        "EVAL", RELEASE, entries.size,
        *entries.map(&:key),
        *entries.map(&:owner),
      )
    end

    def strip(envelope)
      envelope.delete(FIELD)
      envelope
    end
  end
end
