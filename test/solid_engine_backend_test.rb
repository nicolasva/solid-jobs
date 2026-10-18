# frozen_string_literal: true

require "test_helper"
require "tmpdir"

ENV["SOLID_JOBS_TEST_OUT"] ||= File.join(Dir.tmpdir, "solid-jobs-out-#{Process.pid}.txt")

# Conformance tests against a real solid-engine-server binary.
# Set SOLID_ENGINE_BIN to run them; they are skipped otherwise.
class SolidEngineBackendTest < Minitest::Test
  BIN = ENV["SOLID_ENGINE_BIN"]

  def setup
    super

    skip "SOLID_ENGINE_BIN not set" unless BIN && File.executable?(BIN)
    @dir = Dir.mktmpdir("solid-engine-test")
    @sock = File.join(@dir, "engine.sock")
    File.write(File.join(@dir, "engine.toml"), <<~TOML)
      [server]
      unix_socket = "#{@sock}"
    TOML
    @pid = spawn(BIN, "--config", File.join(@dir, "engine.toml"), out: File::NULL, err: File::NULL)
    50.times { break if File.exist?(@sock); sleep 0.1 }
    @backend = SolidJobs::Backends::SolidEngine.new(url: "unix://#{@sock}")
  end

  def teardown
    @backend&.close
    if @pid
      Process.kill("TERM", @pid)
      Process.wait(@pid)
    end
    FileUtils.remove_entry(@dir) if @dir
    super
  end

  def envelope(extra = {})
    { "task" => "Demo", "arguments" => [1], "channel" => "default" }.merge(extra)
  end

  def test_enqueue_claim_ack
    result = @backend.enqueue(envelope)
    refute result[:duplicate]
    lease = @backend.claim
    assert_equal [1], lease.envelope["arguments"]
    assert_equal result[:id], lease.envelope["id"]
    assert @backend.ack(lease)
    assert_equal :completed, @backend.job(result[:id])[:state]
    assert_nil @backend.claim
  end

  def test_idempotent_enqueue
    id = SecureRandom.uuid
    @backend.enqueue(envelope("id" => id))
    assert @backend.enqueue(envelope("id" => id))[:duplicate]
  end

  def test_nack_retries_and_stale_ack_is_rejected
    @backend.enqueue(envelope)
    first = @backend.claim
    @backend.nack(first, delay: 0)
    error = assert_raises(SolidJobs::Backends::SolidEngine::Error) { @backend.ack(first) }
    assert_equal :stale_reservation, error.code
    second = @backend.claim
    assert_equal 2, second.attempts
    assert_operator second.fencing_token, :>, first.fencing_token
  end

  def test_schedule_extend_cancel_and_stats
    scheduled = @backend.schedule(envelope, run_at: Time.now.to_f + 60)
    assert_nil @backend.claim
    assert_equal :scheduled, @backend.job(scheduled[:id])[:state]
    @backend.cancel(scheduled[:id])
    assert_equal :cancelled, @backend.job(scheduled[:id])[:state]

    @backend.enqueue(envelope)
    lease = @backend.claim(lease_ms: 1_000)
    assert_operator @backend.extend_lease(lease, lease_ms: 60_000), :>, lease.lease_deadline - 1
    stats = @backend.stats
    assert_equal 1, stats[:claimed]
    assert_equal 2, stats[:pushed]
  end

  def test_blueprint_selects_backend
    config = SolidJobs::Blueprint.new
    assert_nil config.engine_backend
    config.backend = :solid_engine
    config.url = "unix://#{@sock}"
    assert config.engine_backend.ping
  end

  class Recorder
    include SolidJobs::Task
    task_options channel: "bulk", max_failures: 1
    RESULTS = Queue.new

    def execute_task(value)
      RESULTS << value
      raise "boom" if value == "fail"
    end
  end

  def routed_config(mode = :thread)
    SolidJobs::Blueprint.new(channels: %w[bulk]).tap do |config|
      config.logger = Logger.new(File::NULL)
      config.concurrency = 1
      config.retry_base_delay = 0.01
      config.backends = {redis: {adapter: :solid_redis}, engine: {adapter: :solid_engine, url: "unix://#{@sock}"}}
      config.backend = :redis
      config.channel_backends = {"bulk" => :engine}
      config.execution_mode = mode
    end
  end

  def test_channel_routing_and_worker_end_to_end
    config = routed_config
    assert config.routed?
    assert_empty config.redis_channel_entries
    SolidJobs.testing!(:disable)
    SolidJobs.use_config(config)
    Recorder.enqueue("ok")
    conductor = SolidJobs::Conductor.new(config: config).start
    assert_equal "ok", Timeout.timeout(5) { Recorder::RESULTS.pop }
    Recorder.enqueue("fail")
    2.times { assert_equal "fail", Timeout.timeout(5) { Recorder::RESULTS.pop } }
    sleep 0.3
    stats = config.backend_adapter(:engine).stats
    assert_equal 1, stats[:dead]
    assert_equal 1, stats[:completed]
    conductor.stop
  ensure
    SolidJobs.reset!
  end

  class FileRecorder
    include SolidJobs::Task
    task_options channel: "bulk", max_failures: 0
    PATH = ENV.fetch("SOLID_JOBS_TEST_OUT", "").freeze

    def execute_task(value)
      File.open(PATH, "a") { |file| file.flock(File::LOCK_EX); file.puts(value) }
    end
  end

  def test_ractor_workers_with_one_engine_client_each
    out = ENV.fetch("SOLID_JOBS_TEST_OUT")
    FileUtils.rm_f(out)
    config = routed_config(:ractor)
    config.concurrency = 2
    SolidJobs.testing!(:disable)
    SolidJobs.use_config(config)
    values = (1..20).to_a
    values.each { |value| FileRecorder.enqueue(value) }
    conductor = SolidJobs::Conductor.new(config: config).start
    Timeout.timeout(20) { sleep 0.05 until File.exist?(out) && File.readlines(out).size >= values.size }
    results = conductor.stop
    assert_equal values, File.readlines(out).map(&:to_i).sort
    assert_equal values.size, results.sum { |r| r[:processed] }
    assert_equal values.size, config.backend_adapter(:engine).stats[:completed]
  ensure
    FileUtils.rm_f(out) if out
    SolidJobs.reset!
  end

  def test_execution_mode_is_validated
    assert_raises(ArgumentError) { SolidJobs::Blueprint.new.execution_mode = :fork }
  end

  def test_contract_capabilities
    assert @backend.supports?(:fencing_tokens)
    assert_equal 0, @backend.recover
    redis = SolidJobs::Backends::Redis.new(config: SolidJobs::Blueprint.new)
    refute redis.supports?(:fencing_tokens)
    assert_raises(SolidJobs::Backends::NotSupported) { redis.get_job("x") }
  end
end
