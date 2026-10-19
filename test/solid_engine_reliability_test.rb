# frozen_string_literal: true

require "test_helper"
require "tmpdir"
require "socket"
require "timeout"
require_relative "support/reliability_task"

# Crash and recovery scenarios against a real solid-engine-server and real
# worker processes. Every test checks the fate of each job, not just that a
# process came back. Needs SOLID_ENGINE_BIN; skipped otherwise.
class SolidEngineReliabilityTest < Minitest::Test
  BIN = ENV["SOLID_ENGINE_BIN"]
  WORKER = File.expand_path("support/reliability_worker.rb", __dir__)
  LIB = File.expand_path("../lib", __dir__)
  LEASE_MS = 1_500

  def setup
    super
    skip "SOLID_ENGINE_BIN not set" unless BIN && File.executable?(BIN)
    @dir = Dir.mktmpdir("se-rel-", "/tmp")
    @log = File.join(@dir, "tasks.log")
    FileUtils.touch(@log)
    ENV["RELIABILITY_LOG"] = @log
    @pids = []
  end

  def teardown
    @pids.each { |pid| kill(pid, "KILL") }
    @pids.each { |pid| reap(pid) }
    @client&.close
    FileUtils.remove_entry(@dir) if @dir
    %w[RELIABILITY_LOG RELIABILITY_SLOW RELIABILITY_SLOW_INDEX].each { |key| ENV.delete(key) }
    super
  end

  def start_server(durability: "memory", port: nil)
    @socket = File.join(@dir, "engine.sock")
    config = File.join(@dir, "engine.toml")
    body = +"[server]\nunix_socket = \"#{@socket}\"\n"
    body << "tcp_addr = \"127.0.0.1:#{port}\"\n" if port
    body << "\n[storage]\ndurability = \"#{durability}\"\ndata_dir = \"#{File.join(@dir, "data")}\"\n" unless durability == "memory"
    File.write(config, body)
    FileUtils.rm_f(@socket)
    @server = spawn(BIN, "--config", config, out: File.join(@dir, "engine.log"), err: [:child, :out])
    @pids << @server
    Timeout.timeout(10) { sleep 0.02 until File.exist?(@socket) }
    @server
  end

  def url = "unix://#{@socket}"

  def client(target = url)
    @client&.close
    @client = SolidJobs::Backends::SolidEngine.new(url: target, lease_ms: LEASE_MS)
  end

  def enqueue(count)
    @client = client
    ids = Array.new(count) { |index| SecureRandom.uuid.tap { |id| @client.enqueue(
      {"id" => id, "task" => "ReliabilityTask", "arguments" => [index], "channel" => "rel", "max_failures" => 0},
      max_attempts: 5,
    ) } }
    ids
  end

  def start_worker(env = {})
    environment = {"RELIABILITY_URL" => url, "RELIABILITY_LOG" => @log, "RELIABILITY_LEASE_MS" => LEASE_MS.to_s}.merge(env)
    reader, writer = IO.pipe
    pid = spawn(environment, RbConfig.ruby, "-I", LIB, WORKER, out: writer, err: File.join(@dir, "worker-#{@pids.size}.err"))
    writer.close
    @pids << pid
    Timeout.timeout(20) { loop { break if reader.gets.to_s.include?("READY") } }
    pid
  end

  def kill(pid, signal)
    Process.kill(signal, pid)
  rescue Errno::ESRCH
    nil
  end

  def reap(pid)
    Timeout.timeout(10) { Process.wait(pid) }
  rescue Errno::ECHILD, Timeout::Error
    nil
  end

  def log_lines = File.readlines(@log, chomp: true)

  def started(index) = log_lines.count { |line| line.end_with?(" start #{index}") }

  def done(index) = log_lines.count { |line| line.end_with?(" done #{index}") }

  def wait_for(timeout: 30, message: "condition")
    Timeout.timeout(timeout) { sleep 0.05 until yield }
  rescue Timeout::Error
    flunk "timed out waiting for #{message}\n#{log_lines.last(20).join("\n")}"
  end

  def states(ids) = ids.map { |id| state(id) }

  def state(id)
    @client.job(id)[:state]
  rescue SolidJobs::Backends::SolidEngine::Error => error
    raise unless error.code == :not_found

    :missing
  end

  # P0: a worker killed (SIGKILL) while running a job. The lease expires, the
  # job is claimed again by another worker and runs to completion: at-least-once.
  def test_worker_sigkill_mid_job_is_redelivered
    start_server
    ids = enqueue(6)
    victim = start_worker("RELIABILITY_SLOW" => "30", "RELIABILITY_SLOW_INDEX" => "0")
    wait_for(message: "job 0 to start") { started(0) >= 1 }
    killed_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    kill(victim, "KILL")
    reap(victim)

    start_worker
    wait_for(message: "every job to complete") { states(ids).all?(:completed) }
    recovery = Process.clock_gettime(Process::CLOCK_MONOTONIC) - killed_at

    ids.each_index { |index| assert_operator done(index), :>=, 1, "job #{index} never completed" }
    assert_operator started(0), :>=, 2, "job 0 should have been executed again"
    assert_equal 1, done(0), "job 0 completes once (the killed run never finished)"
    assert_operator recovery, :<, (LEASE_MS / 1000.0) + 10, "recovery slower than lease + margin"
  end

  # P0: execution finished but the ACK never reached the engine (worker killed
  # between the two). The job is delivered again; the old reservation is fenced.
  def test_lost_ack_redelivers_and_fences_the_old_reservation
    start_server
    id = enqueue(1).first
    first = client.claim("rel")
    assert_equal id, first.id
    second = client.claim("rel", wait_ms: LEASE_MS + 5_000)
    assert_equal id, second.id
    assert_equal 2, second.attempt
    error = assert_raises(SolidJobs::Backends::SolidEngine::Error) { client.ack(first) }
    assert_equal :stale_reservation, error.code
    assert client.ack(second)
    assert_equal :completed, state(id)
  end

  # P0: TCP connection dropped between a task finishing and its ACK. The worker
  # must survive, and the job must end up completed exactly through redelivery.
  def test_connection_loss_during_ack_keeps_worker_alive
    start_server(port: free_port)
    proxy = DroppingProxy.new(@proxy_target = @tcp_port)
    ids = enqueue(4)
    proxy.drop_connections_after(:claim)
    worker = start_worker("RELIABILITY_URL" => "tcp://127.0.0.1:#{proxy.port}")
    wait_for(timeout: 60, message: "every job to complete") { states(ids).all?(:completed) }

    ids.each_index { |index| assert_operator done(index), :>=, 1 }
    assert Process.kill(0, worker), "worker process died"
    assert_equal 0, File.read(File.join(@dir, "worker-1.err")).scan(/Error|error/).size,
                 File.read(File.join(@dir, "worker-1.err"))
  ensure
    proxy&.close
  end

  # The ACK itself fails with a connection error: the worker thread must survive
  # and the job is redelivered after its lease expires.
  def test_ack_connection_error_does_not_kill_the_worker
    start_server
    id = enqueue(1).first
    config = SolidJobs::Blueprint.new(channels: %w[rel])
    config.logger = Logger.new(File::NULL)
    config.backend = :solid_engine
    config.backends = {solid_engine: {adapter: :solid_engine, url: url, lease_ms: LEASE_MS}}
    worker = SolidJobs::Backends::Worker.new(config: config, backend_name: :solid_engine, channels: {"rel" => 1})
    backend = worker.instance_variable_get(:@backend)
    failures = 0
    backend.define_singleton_method(:ack) { |_claim| failures += 1; raise Errno::ECONNRESET }
    stop = false
    thread = Thread.new { worker.run(stop: -> { stop }) }
    wait_for(message: "ack to fail") { failures >= 1 }
    sleep 0.3
    assert thread.alive?, "worker thread died on an ACK connection error"
    stop = true
    thread.join(5)
    assert_equal :claimed, state(id), "unacknowledged job stays reserved until its lease expires"
  end

  # Durability is a server setting: with strict durability a SIGKILLed server
  # keeps confirmed jobs; with memory durability they are gone (documented).
  def test_server_restart_strict_durability_keeps_jobs
    server = start_server(durability: "strict")
    ids = enqueue(20)
    kill(server, "KILL")
    reap(server)
    @pids.delete(server)

    start_server(durability: "strict")
    client
    assert_empty states(ids).select { |s| s == :missing }, "strict durability lost confirmed jobs"
    start_worker
    wait_for(message: "recovered jobs to complete") { states(ids).all?(:completed) }
    ids.each_index { |index| assert_operator done(index), :>=, 1 }
  end

  def test_server_restart_memory_durability_loses_jobs
    server = start_server(durability: "memory")
    ids = enqueue(5)
    kill(server, "KILL")
    reap(server)
    @pids.delete(server)

    start_server(durability: "memory")
    client
    assert_equal ids.size, states(ids).count(:missing), "memory durability must not claim persistence"
  end

  # P1: TERM = quiet + stop; the active job finishes and is ACKed, nothing new is claimed.
  def test_clean_stop_lets_the_active_job_finish
    start_server
    ids = enqueue(8)
    worker = start_worker("RELIABILITY_SLOW" => "1.5", "RELIABILITY_SLOW_INDEX" => "0")
    wait_for(message: "job 0 to start") { started(0) >= 1 }
    kill(worker, "TERM")
    reap(worker)
    @pids.delete(worker)

    assert_equal 1, done(0), "active job must finish before stopping"
    assert_equal :completed, state(ids[0])
    remaining = states(ids).count(:completed)
    assert_operator remaining, :<, ids.size, "stopped worker kept claiming new jobs"
  end

  private

  def free_port
    server = TCPServer.new("127.0.0.1", 0)
    @tcp_port = server.local_address.ip_port
    server.close
    @tcp_port
  end

  # TCP proxy that can sever every client connection once.
  class DroppingProxy
    attr_reader :port

    def initialize(target_port)
      @target_port = target_port
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.local_address.ip_port
      @connections = []
      @drop_after = nil
      @mutex = Mutex.new
      @acceptor = Thread.new { accept_loop }
    end

    # Sever all connections shortly after traffic starts, then behave normally.
    def drop_connections_after(_event, delay: 0.7)
      Thread.new do
        sleep delay
        @mutex.synchronize { @connections.each { |pair| pair.each { |s| s.close rescue nil } }; @connections.clear }
      end
    end

    def close
      @server.close rescue nil
      @mutex.synchronize { @connections.each { |pair| pair.each { |s| s.close rescue nil } } }
    end

    private

    def accept_loop
      loop do
        client = @server.accept
        upstream = TCPSocket.new("127.0.0.1", @target_port)
        @mutex.synchronize { @connections << [client, upstream] }
        [[client, upstream], [upstream, client]].each do |from, to|
          Thread.new do
            IO.copy_stream(from, to)
          rescue StandardError
            nil
          ensure
            to.close rescue nil
          end
        end
      end
    rescue IOError, Errno::EBADF
      nil
    end
  end
end
