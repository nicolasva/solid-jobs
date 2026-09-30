# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "json"
require "securerandom"
require "solid_jobs"

class SolidJobsHotPathJob
  include SolidJobs::Job

  def perform(_value)
    nil
  end
end

class PhaseStats
  attr_reader :allocations, :elapsed, :operations

  def initialize
    @allocations = 0
    @elapsed = 0.0
    @operations = 0
  end

  def record(started_at, allocated_before)
    @elapsed += Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    @allocations += GC.stat(:total_allocated_objects) - allocated_before
    @operations += 1
  end

  def allocations_per_job
    allocations.to_f / operations
  end

  def microseconds_per_job
    elapsed * 1_000_000 / operations
  end
end

class SolidJobsHotPathProfile
  PHASES = %i[
    reserve
    deserialize
    metrics_register
    dispatch_perform_wrapper
    metrics_clear
    ack
  ].freeze

  def initialize
    @jobs = Integer(ENV.fetch("HOT_PATH_JOBS", "10000"))
    @warmup = Integer(ENV.fetch("HOT_PATH_WARMUP", "500"))
    redis = SolidRedis::Config.new(
      url: ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0"),
      timeout: 1,
    )
    @config = SolidJobs::Config.new(redis: redis, concurrency: 1)
    @identity = "hot-path:#{Process.pid}:#{SecureRandom.hex(6)}"
    @fetch = SolidJobs::Fetch.new(@config, identity: @identity, processor_id: 0)
    @worker = SolidJobs::Worker.new(redis_config: redis, config: @config)
    @processor = SolidJobs::Processor.new(
      @config,
      identity: @identity,
      processor_id: 0,
    )
  end

  def run
    redis.call("FLUSHDB")
    seed(@warmup + @jobs)
    @warmup.times { execute_once }
    GC.start
    stats = PHASES.to_h { |phase| [phase, PhaseStats.new] }
    @jobs.times { profile_once(stats) }
    assert_clean
    puts render(stats)
  ensure
    @config.close
  end

  private

  def execute_once
    unit = @fetch.retrieve
    payload = unit.job
    @processor.send(:register_work, unit, payload)
    @worker.perform(payload)
    @processor.send(:clear_work)
    raise "Warmup ACK was fenced" unless unit.acknowledge
  end

  def profile_once(stats)
    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    unit = @fetch.retrieve
    stats.fetch(:reserve).record(started, allocated)

    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    payload = unit.job
    stats.fetch(:deserialize).record(started, allocated)

    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    @processor.send(:register_work, unit, payload)
    stats.fetch(:metrics_register).record(started, allocated)

    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    @worker.perform(payload)
    stats.fetch(:dispatch_perform_wrapper).record(started, allocated)

    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    @processor.send(:clear_work)
    stats.fetch(:metrics_clear).record(started, allocated)

    allocated = GC.stat(:total_allocated_objects)
    started = monotonic_time
    acknowledged = unit.acknowledge
    stats.fetch(:ack).record(started, allocated)
    raise "ACK was rejected by reservation fencing" unless acknowledged
  end

  def seed(count)
    now = (Time.now.to_f * 1_000).to_i
    (0...count).each_slice(1_000) do |indices|
      payloads = indices.map do |index|
        JSON.generate(
          "class" => "SolidJobsHotPathJob",
          "args" => [index],
          "queue" => "default",
          "jid" => format("%024x", index),
          "retry" => false,
          "created_at" => now,
          "enqueued_at" => now,
        )
      end
      redis.call("LPUSH", "queue:default", *payloads)
    end
  end

  def assert_clean
    checks = {
      ready: redis.call("LLEN", "queue:default"),
      reserved: redis.call("LLEN", "#{@identity}:reserved:0"),
      reservations: redis.call("HLEN", "#{@identity}:reservations"),
      attempts: redis.call("HLEN", "solid-jobs:attempts"),
    }
    dirty = checks.reject { |_, count| count.zero? }
    raise "Hot-path profile left Redis state behind: #{dirty.inspect}" unless dirty.empty?
  end

  def render(stats)
    rows = PHASES.map do |phase|
      values = stats.fetch(phase)
      format(
        "| `%s` | %.3f | %.2f |",
        phase,
        values.microseconds_per_job,
        values.allocations_per_job,
      )
    end
    total_us = stats.values.sum(&:microseconds_per_job)
    total_allocations = stats.values.sum(&:allocations_per_job)
    <<~MARKDOWN
      # SolidJobs hot-path profile

      **Jobs:** #{@jobs}; **Ruby:** #{RUBY_VERSION}; **reliable fetch:** enabled.

      | Phase | Time/job (us) | Allocations/job |
      |---|---:|---:|
      #{rows.join("\n")}
      | **Total framework path** | **#{format("%.3f", total_us)}** | **#{format("%.2f", total_allocations)}** |

      The job body is intentionally empty. Measurements include Redis transport
      allocations but do not attempt to optimize or credit application work.
    MARKDOWN
  end

  def redis
    @config.redis_pool
  end

  def monotonic_time
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end
end

SolidJobsHotPathProfile.new.run
