# frozen_string_literal: true

require_relative "test_helper"
require "stringio"

class InstrumentationTest < Minitest::Test
  FakeEvent = Data.define(:value) do
    def to_h
      { name: "job.started", schema: 1, payload: { job_id: value } }
    end
  end

  class RecordingRedisPool
    attr_reader :commands
    attr_accessor :failure, :redis_time

    def initialize
      @commands = []
      @closed = false
      @redis_time = ["5000", "500000"]
    end

    def with
      raise failure if failure

      yield self
    end

    def pipelined
      raise failure if failure

      yield self
      commands.map { "1-0" }
    end

    def call(*command)
      commands << command
      redis_time if command == ["TIME"]
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end
  end

  class RecordingRedisConfig
    attr_reader :pool, :options

    def initialize
      @pool = RecordingRedisPool.new
    end

    def new_pool(**options)
      @options = options
      pool
    end
  end

  class RecordingInstrumenter
    attr_reader :events

    def initialize
      @events = []
    end

    def instrument(name, payload)
      @events << [name, payload]
    end
  end

  class FailingInstrumenter
    def self.instrument(_name, _payload)
      raise "telemetry unavailable"
    end
  end

  def test_default_instrumenter_is_a_no_op
    assert_same SolidJobs::NullInstrumenter, SolidJobs.instrumenter
    assert SolidJobs::Instrumentation.emit(
      SolidJobs.config,
      :started,
      { "id" => "job-1", "task" => "ExampleJob", "channel" => "default" },
    )
  end

  def test_emit_uses_only_contract_fields_and_common_process_identity
    recorder = RecordingInstrumenter.new
    SolidJobs.instrumenter = recorder
    envelope = {
      "id" => "job-1",
      "task" => "ExampleJob",
      "channel" => "critical",
      "arguments" => ["secret"],
    }

    SolidJobs::Instrumentation.emit(
      SolidJobs.config,
      :started,
      envelope,
      reservation_id: "claim",
      attempt: 2,
      ractor_id: 1,
      worker_id: 1,
    )

    name, payload = recorder.events.fetch(0)
    assert_equal "job.started", name
    assert_equal(
      %i[attempt job_class job_id node_id queue ractor_id reservation_id worker_id],
      payload.keys.sort,
    )
    assert_equal SolidJobs.config.identity, payload.fetch(:node_id)
    refute_includes payload.values, envelope["arguments"]
  end

  def test_instrumenter_failure_is_logged_and_never_propagated
    output = StringIO.new
    SolidJobs.config.logger = Logger.new(output)
    SolidJobs.instrumenter = FailingInstrumenter

    result = SolidJobs::Instrumentation.emit(
      SolidJobs.config,
      :failed,
      { "id" => "job-1", "task" => "ExampleJob", "channel" => "default" },
    )

    refute result
    assert_match(/telemetry failed/, output.string)
  end

  def test_execution_halt_from_instrumentation_is_not_swallowed
    instrumenter = Class.new do
      def self.instrument(_name, _payload)
        raise SolidJobs::ExecutionHalt
      end
    end
    SolidJobs.instrumenter = instrumenter

    assert_raises(SolidJobs::ExecutionHalt) do
      SolidJobs::Instrumentation.emit(
        SolidJobs.config,
        :started,
        { "id" => "job-1", "task" => "ExampleJob", "channel" => "default" },
      )
    end
  end

  def test_execution_halt_from_failure_logging_is_not_swallowed
    logger = Object.new
    logger.define_singleton_method(:warn) { |_message| raise SolidJobs::ExecutionHalt }
    SolidJobs.config.logger = logger
    SolidJobs.instrumenter = FailingInstrumenter

    assert_raises(SolidJobs::ExecutionHalt) do
      SolidJobs::Instrumentation.emit(
        SolidJobs.config,
        :started,
        { "id" => "job-1", "task" => "ExampleJob", "channel" => "default" },
      )
    end
  end

  def test_ractor_snapshot_preserves_identity_and_shareable_instrumenter
    SolidJobs.instrumenter = FailingInstrumenter
    snapshot = SolidJobs.config.ractor_snapshot
    restored = SolidJobs::Blueprint.from_ractor_snapshot(snapshot)

    assert_equal SolidJobs.config.identity, restored.identity
    assert_same FailingInstrumenter, restored.instrumenter
  ensure
    restored&.close
  end

  def test_non_shareable_instrumenter_is_rejected_before_ractors_start
    SolidJobs.instrumenter = RecordingInstrumenter.new

    error = assert_raises(ArgumentError) { SolidJobs.config.ractor_snapshot }

    assert_match(/Ractor-shareable/, error.message)
  end

  def test_redis_streams_exporter_is_explicit_and_uses_solid_jobs_redis_config
    assert_nil SolidJobs.redis_streams_exporter

    SolidJobs.config.redis_streams_enabled = true
    SolidJobs.config.redis_streams_key = "custom:telemetry"
    SolidJobs.config.redis_streams_pool_size = 2
    SolidJobs.config.redis_streams_pool_timeout = 0.25
    exporter = SolidJobs.redis_streams_exporter

    assert_instance_of SolidJobs::RedisStreamsExporter, exporter
    assert_equal "custom:telemetry", exporter.health.fetch(:stream)
  ensure
    exporter&.shutdown
  end

  def test_redis_streams_exporter_publishes_json_and_trims_each_batch
    redis = RecordingRedisConfig.new
    wall_now = 2_000.0
    exporter = SolidJobs::RedisStreamsExporter.new(
      redis_config: redis,
      stream: "events",
      pool_size: 2,
      pool_timeout: 0.25,
      clock: -> { wall_now },
      monotonic_clock: -> { 10.0 },
    )

    assert exporter.export([FakeEvent.new("a"), FakeEvent.new("b")])

    assert_equal({ size: 2, timeout: 0.25 }, redis.options)
    assert_equal 4, redis.pool.commands.size
    assert_equal ["TIME"], redis.pool.commands.fetch(0)
    first = redis.pool.commands.fetch(1)
    assert_equal ["XADD", "events", "*", "event"], first.first(4)
    assert_equal "a", JSON.parse(first.fetch(4)).dig("payload", "job_id")
    assert_equal ["XTRIM", "events", "MINID", "~", "4100500-0"], redis.pool.commands.last
    health = exporter.health
    assert_equal 2, health.fetch(:accepted)
    assert_equal 2, health.fetch(:exported)
    assert_equal 0, health.fetch(:dropped)
    assert_equal true, health.fetch(:connected)
    assert health.frozen?
    assert health.fetch(:last_success).frozen?
    assert_raises(FrozenError) { health[:connected] = false }
  ensure
    exporter&.shutdown
  end

  def test_redis_streams_exporter_counts_abandoned_batch_and_recovery
    redis = RecordingRedisConfig.new
    wall_now = 2_000.0
    monotonic_now = 10.0
    exporter = SolidJobs::RedisStreamsExporter.new(
      redis_config: redis,
      clock: -> { wall_now },
      monotonic_clock: -> { monotonic_now },
    )
    redis.pool.failure = SolidRedis::ConnectionError.new("offline")

    assert_raises(SolidRedis::ConnectionError) { exporter.export([FakeEvent.new("lost")]) }
    failed = exporter.health
    assert_equal 1, failed.fetch(:accepted)
    assert_equal 1, failed.fetch(:dropped)
    assert_equal 1, failed.fetch(:errors)
    assert_equal false, failed.fetch(:connected)
    assert_equal 2_000.0, failed.dig(:last_failure, :at)

    redis.pool.failure = nil
    wall_now = 2_002.0
    monotonic_now = 12.0
    assert exporter.export([FakeEvent.new("recovered")])
    recovered = exporter.health
    assert_equal 1, recovered.fetch(:recoveries)
    assert_equal true, recovered.fetch(:connected)
    assert_equal 1, recovered.fetch(:exported)
    assert_equal 2.0, recovered.dig(:last_failure, :age_seconds)
  ensure
    exporter&.shutdown
  end

  def test_redis_streams_exporter_shutdown_closes_its_dedicated_pool
    redis = RecordingRedisConfig.new
    exporter = SolidJobs::RedisStreamsExporter.new(redis_config: redis)

    exporter.shutdown

    assert redis.pool.closed?
    assert exporter.health.fetch(:closed)
    assert_raises(IOError) { exporter.export([FakeEvent.new("late")]) }
    assert_equal 1, exporter.health.fetch(:dropped)
  end

  def test_batch_planned_and_lab_publications_emit_after_acceptance
    recorder = RecordingInstrumenter.new
    SolidJobs.instrumenter = recorder
    ids = SolidJobs::Publisher.new(config: SolidJobs.config).publish_many(
      "task" => "ExampleJob",
      "arguments" => [[1], [2]],
      "channel" => "default",
      "run_at" => [Time.now.to_f + 10, Time.now.to_f + 20],
      "chunk_size" => 1,
    )

    assert_equal 2, ids.size
    assert_equal(
      %w[job.enqueued job.journaled job.enqueued job.journaled],
      recorder.events.map(&:first),
    )
    assert recorder.events.all? { |_name, payload| ids.include?(payload.fetch(:job_id)) }
  end

  def test_publisher_return_value_survives_instrumenter_failure
    SolidJobs.instrumenter = FailingInstrumenter

    job_id = SolidJobs::Publisher.new(config: SolidJobs.config).publish(
      "task" => "ExampleJob",
      "arguments" => [],
      "channel" => "default",
    )

    assert_kind_of String, job_id
    assert_equal 1, SolidJobs::Lab.captured_for(ExampleJob).size
  end

  def test_failed_recovery_does_not_emit_a_recovered_event
    recorder = RecordingInstrumenter.new
    claimed_key = SolidJobs::Keyspace.claimed("remote:999999:dead", 0)
    redis_pool = Object.new
    redis_pool.define_singleton_method(:call) do |command, *_arguments|
      case command
      when "SCAN"
        ["0", [claimed_key]]
      when "HGET"
        nil
      when "EVAL"
        raise SolidRedis::ConnectionError, "recovery unavailable"
      end
    end
    config = Struct.new(:redis_pool, :instrumenter, :identity, :logger).new(
      redis_pool,
      recorder,
      "local:123:producer",
      Logger.new(StringIO.new),
    )

    assert_raises(SolidRedis::ConnectionError) do
      SolidJobs::Recovery.new(config: config).call
    end
    assert_empty recorder.events
  end

  def test_malformed_recovered_payload_does_not_change_recovery_result
    recorder = RecordingInstrumenter.new
    output = StringIO.new
    claimed_key = SolidJobs::Keyspace.claimed("remote:999999:dead", 0)
    replies = ["not-json", nil]
    redis_pool = Object.new
    redis_pool.define_singleton_method(:call) do |command, *_arguments|
      case command
      when "SCAN"
        ["0", [claimed_key]]
      when "HGET"
        nil
      when "EVAL"
        replies.shift
      when "DEL"
        1
      end
    end
    config = Struct.new(:redis_pool, :instrumenter, :identity, :logger).new(
      redis_pool,
      recorder,
      "local:123:producer",
      Logger.new(output),
    )

    assert_equal 1, SolidJobs::Recovery.new(config: config).call
    assert_empty recorder.events
    assert_match(/telemetry failed/, output.string)
  end

  def test_process_observations_cache_success_and_report_stale_after_failure
    now = 10.0
    cpu_values = [1.5, RuntimeError.new("clock unavailable")]
    sources = {
      cpu_time: -> {
        value = cpu_values.shift
        raise value if value.is_a?(Exception)

        value
      },
      rss: -> { raise "unsupported" },
      gc_count: -> { 2 },
      gc_time: -> { 3 },
      allocations: -> { 4 },
    }
    observations = SolidJobs::Observations.new(
      clock: -> { now },
      sources: sources,
    )

    first = observations.process(node_id: "node", observed_at: now)
    now = 13.25
    second = observations.process(node_id: "node", observed_at: now)

    assert_equal "available", first.dig(:metrics, :cpu_time, :status)
    assert_equal "unavailable", first.dig(:metrics, :rss, :status)
    assert_nil first.dig(:metrics, :rss, :value)
    assert_equal "stale", second.dig(:metrics, :cpu_time, :status)
    assert_equal 1.5, second.dig(:metrics, :cpu_time, :value)
    assert_equal 10.0, second.dig(:metrics, :cpu_time, :observed_at)
    assert_equal 3.25, second.dig(:metrics, :cpu_time, :age_seconds)
    assert_equal "unavailable", second.dig(:metrics, :rss, :status)
  end

  def test_ractor_and_redis_observations_only_expose_authorized_state
    observations = SolidJobs::Observations.new(sources: {
      cpu_time: -> { 1 }, rss: -> { 1 }, gc_count: -> { 1 },
      gc_time: -> { 1 }, allocations: -> { 1 },
    })
    work = {
      "1" => JSON.generate(
        "envelope" => {
          "id" => "job-1",
          "task" => "SecretJob",
          "arguments" => ["secret"],
        },
      ),
    }

    ractors = observations.ractors(
      node_id: "node",
      concurrency: 2,
      work: work,
      observed_at: 20.0,
    )
    redis = observations.redis(
      node_id: "node",
      concurrency: 2,
      statuses: {"1" => "disconnected"},
      observed_at: 20.0,
    )

    assert_equal [0, 1], ractors.map { |payload| payload.fetch(:ractor_id) }
    assert_equal ["idle", "busy"], ractors.map { |payload| payload.fetch(:state) }
    assert_equal [[], ["job-1"]], ractors.map { |payload| payload.fetch(:current_job_ids) }
    refute_includes ractors.join, "secret"
    assert_equal %w[unavailable disconnected], redis.map { |payload| payload.fetch(:connection_status) }
    assert(redis.all? do |payload|
      payload.fetch(:metrics).values.all? do |metric|
        metric.fetch(:status) == "unavailable" && metric.fetch(:value).nil?
      end
    end)
  end

  def test_heartbeat_publishes_each_observed_redis_transition
    recorder = RecordingInstrumenter.new
    config = Struct.new(:instrumenter, :logger).new(recorder, Logger.new(StringIO.new))
    sources = {
      cpu_time: -> { 1 }, rss: -> { 1 }, gc_count: -> { 1 },
      gc_time: -> { 1 }, allocations: -> { 1 },
    }
    heartbeat = SolidJobs::Heartbeat.new(
      config,
      identity: "node",
      started_at: 1.0,
      concurrency: 1,
      observations: SolidJobs::Observations.new(sources: sources),
    )

    heartbeat.observe_redis(0, "connected", observed_at: 2.0)
    heartbeat.observe_redis(0, "disconnected", observed_at: 3.0)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 1
    sleep 0.001 while recorder.events.size < 2 &&
      Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline

    assert_equal(
      %w[connected disconnected],
      recorder.events.map { |_name, payload| payload.fetch(:connection_status) },
    )
    assert recorder.events.all? { |name, _payload| name == "redis.observed" }
  end

  def test_heartbeat_observes_process_and_workers_when_redis_write_fails
    recorder = RecordingInstrumenter.new
    redis_pool = Object.new
    redis_pool.define_singleton_method(:call) do |*_arguments|
      raise SolidRedis::ConnectionError, "offline"
    end
    config = Struct.new(:instrumenter, :logger, :redis_pool, :channels).new(
      recorder,
      Logger.new(StringIO.new),
      redis_pool,
      ["default"],
    )
    sources = {
      cpu_time: -> { 1 }, rss: -> { 1 }, gc_count: -> { 1 },
      gc_time: -> { 1 }, allocations: -> { 1 },
    }
    heartbeat = SolidJobs::Heartbeat.new(
      config,
      identity: "node",
      started_at: 1.0,
      concurrency: 1,
      observations: SolidJobs::Observations.new(sources: sources),
    )

    assert_raises(SolidRedis::ConnectionError) do
      heartbeat.beat(redis: {"0" => "disconnected"}, phase: :stopped)
    end

    assert_equal(
      %w[process.observed ractor.observed redis.observed],
      recorder.events.map(&:first),
    )
    assert_equal "stopped", recorder.events.fetch(1).last.fetch(:state)
    assert_equal "disconnected", recorder.events.fetch(2).last.fetch(:connection_status)
  end

  def test_engine_reports_successful_and_failed_redis_operations_to_heartbeat
    claims = Object.new
    calls = 0
    claims.define_singleton_method(:next) do
      calls += 1
      raise SolidRedis::ConnectionError, "offline" if calls == 2

      nil
    end
    claims.define_singleton_method(:connection_failed!) { true }
    messages = []
    heartbeat = Object.new
    heartbeat.define_singleton_method(:send) do |message, move: nil|
      messages << message
      move
    end
    engine = SolidJobs::Engine.allocate
    engine.instance_variable_set(:@claims, claims)
    engine.instance_variable_set(:@heartbeat, heartbeat)
    engine.instance_variable_set(:@processor_id, 4)
    engine.instance_variable_set(:@redis_status, nil)
    engine.instance_variable_set(
      :@config,
      Struct.new(:logger).new(Logger.new(StringIO.new)),
    )
    engine.define_singleton_method(:sleep) { |_duration| nil }

    assert_nil engine.send(:retrieve)
    assert_nil engine.send(:retrieve)
    assert_equal [[:redis, 4, "connected"], [:redis, 4, "disconnected"]], messages
  end

  def test_engine_reports_successful_and_failed_requeues_to_heartbeat
    claim_class = Struct.new(:result, :claim_token) do
      def requeue
        raise result if result.is_a?(Exception)

        result
      end
    end
    messages = []
    heartbeat = Object.new
    heartbeat.define_singleton_method(:send) do |message, move: nil|
      messages << message
      move
    end
    engine = SolidJobs::Engine.allocate
    engine.instance_variable_set(:@heartbeat, heartbeat)
    engine.instance_variable_set(:@processor_id, 4)
    engine.instance_variable_set(:@redis_status, nil)
    engine.instance_variable_set(
      :@config,
      Struct.new(:logger).new(Logger.new(StringIO.new)),
    )

    assert engine.send(:requeue, claim_class.new(true, "claim-1"))
    error = SolidRedis::ConnectionError.new("offline")
    assert_raises(SolidRedis::ConnectionError) do
      engine.send(:requeue, claim_class.new(error, "claim-2"))
    end
    assert_equal [[:redis, 4, "connected"], [:redis, 4, "disconnected"]], messages
  end

  def test_quiet_and_shutdown_worker_states_never_invent_jobs
    observations = SolidJobs::Observations.new

    quiet = observations.ractors(
      node_id: "node", concurrency: 1, work: {}, quiet: true, observed_at: 1.0,
    ).first
    stopping = observations.ractors(
      node_id: "node", concurrency: 1, work: {}, phase: :stopping, observed_at: 2.0,
    ).first
    stopped = observations.ractors(
      node_id: "node", concurrency: 1, work: {}, phase: :stopped, observed_at: 3.0,
    ).first

    assert_equal ["idle", "waiting", []],
      quiet.values_at(:state, :activity, :current_job_ids)
    assert_equal ["stopping", "stopping", []],
      stopping.values_at(:state, :activity, :current_job_ids)
    assert_equal ["stopped", "stopped", []],
      stopped.values_at(:state, :activity, :current_job_ids)
  end
end

class ExampleJob
  include SolidJobs::Task

  def execute_task
    :ok
  end
end
