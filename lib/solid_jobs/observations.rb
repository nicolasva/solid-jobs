# frozen_string_literal: true

require "json"
require "open3"
require "socket"

module SolidJobs
  class Observations
    PROCESS_METRICS = Ractor.make_shareable({
      cpu_time: ["seconds", "ruby-process-clock"],
      rss: ["bytes", "ps"],
      gc_count: ["collections", "ruby-gc"],
      gc_time: ["nanoseconds", "ruby-gc"],
      allocations: ["objects", "ruby-gc"],
    })
    REDIS_METRICS = Ractor.make_shareable({
      latency: ["seconds", "solid-jobs"],
      commands: ["commands", "solid-jobs"],
      memory: ["bytes", "solid-jobs"],
      connections: ["connections", "solid-jobs"],
    })

    def initialize(
      clock: -> { Time.now.to_f },
      sources: nil
    )
      @clock = clock
      @sources = sources || default_sources
      @last_success = {}
    end

    def process(node_id:, observed_at: @clock.call)
      {
        node_id: node_id,
        process_id: node_id,
        hostname: Socket.gethostname,
        observed_at: observed_at,
        metrics: PROCESS_METRICS.to_h do |name, (unit, source)|
          [name, sample(name, unit, source, observed_at)]
        end,
      }
    end

    def ractors(node_id:, concurrency:, work:, quiet: false, phase: nil, observed_at: @clock.call)
      Array.new(concurrency) do |processor_id|
        ractor_worker(
          node_id: node_id,
          processor_id: processor_id,
          work: work[processor_id.to_s],
          quiet: quiet,
          phase: phase,
          observed_at: observed_at,
        )
      end
    end

    def ractor_worker(
      node_id:, processor_id:, work:, job_id: nil, quiet: false, phase: nil,
      observed_at: @clock.call
    )
      job_id ||= current_job_id(work)
      state, activity = worker_state(job_id, quiet, phase)
      {
        node_id: node_id,
        observed_at: observed_at,
        ractor_id: processor_id,
        worker_id: processor_id,
        executor_id: processor_id,
        state: state,
        activity: activity,
        current_job_ids: job_id ? [job_id] : [],
      }
    end

    def redis(node_id:, concurrency:, statuses:, observed_at: @clock.call)
      Array.new(concurrency) do |processor_id|
        redis_worker(
          node_id: node_id,
          processor_id: processor_id,
          status: statuses.fetch(processor_id.to_s, "unavailable"),
          observed_at: observed_at,
        )
      end
    end

    def redis_worker(node_id:, processor_id:, status:, observed_at: @clock.call)
      {
        node_id: node_id,
        observed_at: observed_at,
        ractor_id: processor_id,
        worker_id: processor_id,
        executor_id: processor_id,
        connection_status: status,
        metrics: REDIS_METRICS.to_h do |name, (unit, source)|
          [name, unavailable(unit, source)]
        end,
      }
    end

    private

    def sample(name, unit, source, observed_at)
      value = @sources.fetch(name).call
      raise RangeError, "#{name} must be non-negative" unless value.is_a?(Numeric) && value.finite? && !value.negative?

      @last_success[name] = [value, observed_at]
      measurement(value, unit, "available", source, observed_at, 0.0)
    rescue StandardError, NotImplementedError
      last = @last_success[name]
      return unavailable(unit, source) unless last

      value, sampled_at = last
      measurement(value, unit, "stale", source, sampled_at, observed_at - sampled_at)
    end

    def measurement(value, unit, status, source, observed_at, age_seconds)
      {
        value: value,
        unit: unit,
        status: status,
        source: source,
        observed_at: observed_at,
        age_seconds: age_seconds,
      }
    end

    def unavailable(unit, source)
      measurement(nil, unit, "unavailable", source, nil, nil)
    end

    def current_job_id(raw)
      return unless raw

      record = JSON.parse(raw)
      direct_job_id = record["job_id"]
      return direct_job_id if direct_job_id.is_a?(String) && !direct_job_id.empty?

      envelope = record.fetch("envelope")
      envelope = JSON.parse(envelope) if envelope.is_a?(String)
      job_id = envelope["id"]
      job_id if job_id.is_a?(String) && !job_id.empty?
    rescue JSON::ParserError, KeyError, TypeError
      nil
    end

    def worker_state(job_id, quiet, phase)
      case phase
      when :stopping then %w[stopping stopping]
      when :stopped then %w[stopped stopped]
      else
        job_id ? %w[busy executing] : ["idle", quiet ? "waiting" : "waiting"]
      end
    end

    def default_sources
      {
        cpu_time: -> { Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) },
        rss: method(:read_rss),
        gc_count: -> { GC.count },
        gc_time: lambda {
          raise NotImplementedError, "GC.total_time unavailable" unless GC.respond_to?(:total_time)

          GC.measure_total_time = true if GC.respond_to?(:measure_total_time=)
          GC.total_time
        },
        allocations: -> { GC.stat.fetch(:total_allocated_objects) },
      }
    end

    def read_rss
      stdout, stderr, status = Open3.capture3("ps", "-o", "rss=", "-p", Process.pid.to_s)
      raise "ps failed: #{stderr.strip}" unless status.success?

      Integer(stdout.strip) * 1024
    end
  end
end
