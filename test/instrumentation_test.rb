# frozen_string_literal: true

require_relative "test_helper"
require "stringio"

class InstrumentationTest < Minitest::Test
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
end

class ExampleJob
  include SolidJobs::Task

  def execute_task
    :ok
  end
end
