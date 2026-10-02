# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

require "json"
require "solid_jobs"

MODE = ENV.fetch("CPU_SCALING_MODE").freeze
CONCURRENCY = Integer(ENV.fetch("CPU_SCALING_CONCURRENCY"))
JOBS = Integer(ENV.fetch("CPU_SCALING_JOBS", "1000"))
ITERATIONS = Integer(ENV.fetch("CPU_SCALING_ITERATIONS", "210000"))
SAMPLE_EVERY = Integer(ENV.fetch("CPU_SCALING_SAMPLE_EVERY", "16"))
REDIS_URL = ENV["CPU_SCALING_REDIS_URL"]&.freeze

raise ArgumentError, "jobs must be divisible by concurrency" unless (JOBS % CONCURRENCY).zero?

module CpuScalingWork
  module_function

  def run(iterations, seed)
    value = seed
    iterations.times do
      value = ((value * 1_664_525) + 1_013_904_223) & 0xffff_ffff
    end
    value
  end
end

class SolidJobsCpuScalingTask
  include SolidJobs::Task

  def execute_task(iterations, seed)
    CpuScalingWork.run(iterations, seed)
  end
end

def percentile(values, ratio)
  values.empty? ? 0 : values[((values.length - 1) * ratio).round]
end

if %w[protocol direct].include?(MODE)
  raise "CPU_SCALING_REDIS_URL is required for #{MODE}" unless REDIS_URL

  seed_config = SolidJobs::Blueprint.new(
    redis: SolidRedis::Config.new(url: REDIS_URL, timeout: 1),
    concurrency: 1,
  )
  seed_config.redis_pool.call("FLUSHDB")
  now = (Time.now.to_f * 1_000).to_i
  (0...JOBS).each_slice(1_000) do |indices|
    payloads = indices.map do |index|
      JSON.generate(
        "task" => "SolidJobsCpuScalingTask",
        "arguments" => [ITERATIONS, index],
        "channel" => "default",
        "id" => format("%024x", index),
        "max_failures" => 0,
        "created_ms" => now,
        "queued_ms" => now,
      )
    end
    seed_config.redis_pool.call("LPUSH", "solid_jobs:channel:default", *payloads)
  end
  seed_config.redis_pool.call("CONFIG", "RESETSTAT")
end

jobs_per_ractor = JOBS / CONCURRENCY
workers = CONCURRENCY.times.map do |worker_index|
  Ractor.new(MODE, jobs_per_ractor, worker_index, REDIS_URL) do |mode, count, index, redis_url|
    if mode == "dispatch"
      config = SolidJobs::Blueprint.new(concurrency: 1)
      executor = SolidJobs::Executor.new(redis_config: config.redis_config, config: config)
      payload = {
        "task" => "SolidJobsCpuScalingTask",
        "arguments" => [ITERATIONS, index],
        "channel" => "benchmark",
        "id" => format("%024x", index),
      }
    elsif mode == "direct" || mode == "protocol"
      redis = SolidRedis::Config.new(url: redis_url, timeout: 1)
      config = SolidJobs::Blueprint.new(redis: redis, concurrency: 1)
      claims = SolidJobs::Claim.new(
        config,
        identity: "cpu-scaling:#{Process.pid}:#{index}",
        processor_id: index,
      )
      executor = SolidJobs::Executor.new(redis_config: redis, config: config) if mode == "direct"
      config.redis_pool.call("PING")
    end
    Ractor.receive
    execution_latencies = []
    reserve_latencies = []
    ack_latencies = []
    count.times do |job_index|
      sample = (job_index % SAMPLE_EVERY).zero?
      if mode == "direct" || mode == "protocol"
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC) if sample
        claim = claims.next
        if sample
          reserve_latencies << (
            (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000
          )
        end
      end
      if mode == "dispatch"
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC) if sample
        executor.execute(payload)
        if sample
          execution_latencies << (
            (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000
          )
        end
      elsif mode == "direct"
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC) if sample
        executor.execute(claim.envelope)
        if sample
          execution_latencies << (
            (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000
          )
        end
      elsif mode == "pure"
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC) if sample
        CpuScalingWork.run(ITERATIONS, index)
        if sample
          execution_latencies << (
            (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000
          )
        end
      end
      if claim
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC) if sample
        raise "Completion fencing rejected current claim" unless claim.complete
        if sample
          ack_latencies << (
            (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000
          )
        end
      end
    end
    config&.close
    {
      execution: execution_latencies,
      reserve: reserve_latencies,
      ack: ack_latencies,
    }
  end
end

GC.start
GC.measure_total_time = true
gc_before = GC.stat
gc_time_before = GC.total_time
allocations_before = GC.stat(:total_allocated_objects)
cpu_before = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
workers.each { |worker| worker.send(:start) }
worker_results = workers.map { |worker| SolidJobs::RactorSupport.value(worker) }
execution_latencies = worker_results.flat_map { |result| result.fetch(:execution) }.sort
reserve_latencies = worker_results.flat_map { |result| result.fetch(:reserve) }.sort
ack_latencies = worker_results.flat_map { |result| result.fetch(:ack) }.sort
elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
cpu_elapsed = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu_before
gc_after = GC.stat
command_calls = if seed_config
  info = seed_config.redis_pool.call("INFO", "commandstats")
  info.scan(/^cmdstat_([^:]+):calls=(\d+)/).to_h
    .transform_values!(&:to_i)
    .reject { |command, _| %w[config info ping].include?(command) }
else
  {}
end
seed_config&.close

puts JSON.generate(
  mode: MODE,
  concurrency: CONCURRENCY,
  throughput: JOBS / elapsed,
  execution_p50_ms: percentile(execution_latencies, 0.50),
  execution_p95_ms: percentile(execution_latencies, 0.95),
  execution_p99_ms: percentile(execution_latencies, 0.99),
  reserve_p50_ms: percentile(reserve_latencies, 0.50),
  reserve_p99_ms: percentile(reserve_latencies, 0.99),
  ack_p50_ms: percentile(ack_latencies, 0.50),
  ack_p99_ms: percentile(ack_latencies, 0.99),
  redis_commands_per_job: command_calls.values.sum.to_f / JOBS,
  redis_scripts_per_job: (
    command_calls.fetch("eval", 0) + command_calls.fetch("evalsha", 0)
  ).to_f / JOBS,
  redis_command_calls: command_calls,
  cpu_percent: cpu_elapsed / elapsed * 100,
  cpu_seconds_per_thousand: cpu_elapsed * 1_000 / JOBS,
  allocations_per_job: (
    GC.stat(:total_allocated_objects) - allocations_before
  ).to_f / JOBS,
  minor_gc_count: gc_after[:minor_gc_count] - gc_before[:minor_gc_count],
  major_gc_count: gc_after[:major_gc_count] - gc_before[:major_gc_count],
  gc_time_ms: (GC.total_time - gc_time_before) / 1_000_000.0,
  heap_live_slots_delta: gc_after[:heap_live_slots] - gc_before[:heap_live_slots],
  heap_free_slots: gc_after[:heap_free_slots],
  malloc_increase_bytes: gc_after[:malloc_increase_bytes],
  elapsed: elapsed,
)
