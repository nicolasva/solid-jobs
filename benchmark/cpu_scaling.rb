# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require_relative "redis_server"

class CpuScalingComparison
  MODES = %w[pure dispatch].freeze
  CONCURRENCY = [1, 2, 4, 8].freeze
  METRICS = %w[
    throughput execution_p50_ms execution_p95_ms execution_p99_ms cpu_percent
    cpu_seconds_per_thousand allocations_per_job minor_gc_count major_gc_count
    gc_time_ms heap_live_slots_delta heap_free_slots malloc_increase_bytes
    peak_rss_mb reserve_p50_ms reserve_p99_ms ack_p50_ms ack_p99_ms
    redis_commands_per_job redis_scripts_per_job
  ].freeze

  def initialize
    @repetitions = Integer(ENV.fetch("CPU_SCALING_REPETITIONS", "6"))
    @modes = ENV.fetch("CPU_SCALING_MODES", MODES.join(",")).split(",")
    @concurrency = ENV.fetch(
      "CPU_SCALING_CONCURRENCY",
      CONCURRENCY.join(","),
    ).split(",").map(&:to_i)
  end

  def run
    @redis = CpuScalingRedis.new.start if (@modes & %w[protocol direct]).any?
    results = @modes.flat_map do |mode|
      @concurrency.map do |concurrency|
        runs = Array.new(@repetitions) do |repetition|
          warn "CPU scaling #{mode} #{concurrency}R (#{repetition + 1}/#{@repetitions})"
          execute(mode, concurrency)
        end
        aggregate(mode, concurrency, runs)
      end
    end
    puts render(results)
  ensure
    @redis&.close
  end

  private

  def execute(mode, concurrency)
    env = {
      "CPU_SCALING_MODE" => mode,
      "CPU_SCALING_CONCURRENCY" => concurrency.to_s,
    }
    env["CPU_SCALING_REDIS_URL"] = @redis.url if @redis
    %w[
      CPU_SCALING_JOBS CPU_SCALING_ITERATIONS CPU_SCALING_SAMPLE_EVERY
    ].each do |name|
      env[name] = ENV[name] if ENV[name]
    end
    command = [RbConfig.ruby, File.join(__dir__, "cpu_scaling_worker.rb")]
    Open3.popen3(env, *command) do |stdin, stdout, stderr, wait_thread|
      stdin.close
      output_thread = Thread.new { stdout.read }
      error_thread = Thread.new { stderr.read }
      peak_rss = 0.0
      until wait_thread.join(0.02)
        peak_rss = [peak_rss, rss_mb(wait_thread.pid)].max
      end
      output = output_thread.value
      errors = error_thread.value
      raise "CPU scaling failed:\n#{output}\n#{errors}" unless wait_thread.value.success?

      JSON.parse(output.lines.last).merge("peak_rss_mb" => peak_rss)
    end
  end

  def aggregate(mode, concurrency, runs)
    {"mode" => mode, "concurrency" => concurrency}.merge(
      METRICS.to_h do |metric|
        [metric, median(runs.map { |run| run.fetch(metric) })]
      end,
    )
  end

  def median(values)
    sorted = values.sort
    middle = sorted.length / 2
    sorted.length.odd? ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2.0
  end

  def render(results)
    rows = results.map do |result|
      baseline = results.find do |candidate|
        candidate["mode"] == result["mode"] && candidate["concurrency"] == 1
      end || result
      efficiency = result["throughput"] /
        (baseline["throughput"] * result["concurrency"]) * 100
      format(
        "| `%s` | %d | %.1f | %.1f%% | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f | %.1f%% | %.2f | %.2f | %.2f | %.1f | %.2f | %.0f | %.0f | %.2f | %.0f | %.0f | %.0f |",
        result["mode"],
        result["concurrency"],
        result["throughput"],
        efficiency,
        result["execution_p50_ms"],
        result["execution_p95_ms"],
        result["execution_p99_ms"],
        result["reserve_p50_ms"],
        result["reserve_p99_ms"],
        result["ack_p50_ms"],
        result["ack_p99_ms"],
        result["cpu_percent"],
        result["cpu_seconds_per_thousand"],
        result["redis_commands_per_job"],
        result["redis_scripts_per_job"],
        result["peak_rss_mb"],
        result["allocations_per_job"],
        result["minor_gc_count"],
        result["major_gc_count"],
        result["gc_time_ms"],
        result["heap_live_slots_delta"],
        result["heap_free_slots"],
        result["malloc_increase_bytes"],
      )
    end
    <<~MARKDOWN
      # Pure Ractor vs SolidJobs dispatch CPU scaling

      **Environment:** Ruby #{RUBY_VERSION} (#{RUBY_PLATFORM});
      #{core_topology}; medians of #{@repetitions} fresh processes.

      | Mode | Ractors | jobs/s | Efficiency | perform p50 | perform p95 | perform p99 | reserve p50 | reserve p99 | ACK p50 | ACK p99 | CPU | CPU-s/1000 | Redis cmd/job | Scripts/job | RSS MiB | Alloc/job | Minor GC | Major GC | GC time ms | Live slots Δ | Free slots | malloc bytes |
      |---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
      #{rows.join("\n")}
    MARKDOWN
  end

  def core_topology
    physical = sysctl("hw.physicalcpu")
    logical = sysctl("hw.logicalcpu")
    performance = sysctl("hw.perflevel0.physicalcpu")
    [
      "#{physical || "?"} physical cores",
      "#{logical || "?"} logical CPUs",
      ("#{performance} performance cores" if performance),
    ].compact.join(", ")
  end

  def sysctl(name)
    output, status = Open3.capture2("sysctl", "-n", name)
    status.success? ? output.strip : nil
  end

  def rss_mb(pid)
    output, = Open3.capture2("ps", "-o", "rss=", "-p", pid.to_s)
    output.to_f / 1_024
  end
end

CpuScalingComparison.new.run
