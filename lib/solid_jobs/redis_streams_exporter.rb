# frozen_string_literal: true

require "json"

module SolidJobs
  class RedisStreamsExporter
    RETENTION_SECONDS = 15 * 60
    ShutdownError = Class.new(IOError)
    private_constant :ShutdownError

    def initialize(
      redis_config:,
      stream: "solid_trace:events:v1",
      pool_size: 1,
      pool_timeout: 0.1,
      clock: -> { Process.clock_gettime(Process::CLOCK_REALTIME) },
      monotonic_clock: -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
    )
      @stream = String(stream).dup.freeze
      raise ArgumentError, "Redis Streams key must not be empty" if @stream.empty?

      @clock = clock
      @monotonic_clock = monotonic_clock
      @pool = redis_config.new_pool(size: Integer(pool_size), timeout: Float(pool_timeout))
      @lock = Mutex.new
      @accepted = 0
      @exported = 0
      @dropped = 0
      @errors = 0
      @recoveries = 0
      @connected = nil
      @last_success_at = nil
      @last_success_monotonic = nil
      @last_failure_at = nil
      @last_failure_monotonic = nil
      @closed = false
    end

    def export(events)
      batch = Array(events)
      return true if batch.empty?

      @lock.synchronize do
        @accepted += batch.size
        raise IOError, "Redis Streams exporter is closed" if @closed
      end
      payloads = batch.map { |event| JSON.generate(event.to_h) }
      cutoff = ((@clock.call - RETENTION_SECONDS) * 1_000).floor
      @pool.pipelined do |pipeline|
        payloads.each { |payload| pipeline.call("XADD", @stream, "*", "event", payload) }
        pipeline.call("XTRIM", @stream, "MINID", "~", "#{cutoff}-0")
      end
      raise ShutdownError, "Redis Streams exporter closed during export" unless record_success(batch.size)

      true
    rescue ShutdownError
      raise
    rescue StandardError, ScriptError
      record_failure(batch&.size || 0)
      raise
    end

    def health
      @lock.synchronize do
        wall_now = @clock.call
        monotonic_now = @monotonic_clock.call
        deep_freeze({
          stream: @stream.dup,
          accepted: @accepted,
          exported: @exported,
          dropped: @dropped,
          errors: @errors,
          connected: @connected,
          recoveries: @recoveries,
          last_success: freshness(@last_success_at, @last_success_monotonic, wall_now, monotonic_now),
          last_failure: freshness(@last_failure_at, @last_failure_monotonic, wall_now, monotonic_now),
          closed: @closed,
        })
      end
    end

    def shutdown
      should_close = @lock.synchronize do
        next false if @closed

        @closed = true
        true
      end
      @pool.close if should_close
      nil
    end

    private

    def record_success(count)
      wall_now = @clock.call
      monotonic_now = @monotonic_clock.call
      @lock.synchronize do
        if @closed
          record_failure_locked(count, wall_now, monotonic_now)
          return false
        end

        @recoveries += 1 if @connected == false
        @connected = true
        @exported += count
        @last_success_at = wall_now
        @last_success_monotonic = monotonic_now
        true
      end
    end

    def record_failure(count)
      wall_now = @clock.call
      monotonic_now = @monotonic_clock.call
      @lock.synchronize { record_failure_locked(count, wall_now, monotonic_now) }
    end

    def record_failure_locked(count, wall_now, monotonic_now)
      @connected = false
      @dropped += count
      @errors += 1
      @last_failure_at = wall_now
      @last_failure_monotonic = monotonic_now
    end

    def freshness(timestamp, monotonic_timestamp, wall_now, monotonic_now)
      {
        at: timestamp,
        age_seconds: monotonic_timestamp ? [monotonic_now - monotonic_timestamp, 0.0].max : nil,
        observed_at: wall_now,
      }
    end

    def deep_freeze(value)
      case value
      when Hash
        value.each { |key, item| deep_freeze(key); deep_freeze(item) }
      when Array
        value.each { |item| deep_freeze(item) }
      end
      value.freeze
    end
  end
end
