# frozen_string_literal: true

require_relative "test_helper"
require "socket"
require "tmpdir"
require_relative "support/warm_pool_fixtures"

module Rails; end unless defined?(Rails)
require_relative "../app/jobs/solid_jobs_generic_executor"

class WarmPoolTest < Minitest::Test
  ROOT = File.expand_path("..", __dir__)
  WORKER_ID = "test#{Process.pid}"

  def self.worker_pid
    @worker_pid ||= begin
      pid = Process.spawn(
        RbConfig.ruby, "-I#{File.join(ROOT, "lib")}", "-rsolid_jobs",
        "-r#{File.join(ROOT, "test/support/warm_pool_fixtures.rb")}",
        File.join(ROOT, "script/warm_worker.rb"), WORKER_ID,
        out: File::NULL, err: File::NULL
      )
      path = format(SolidJobsGenericExecutor::SOCKET_PATH, WORKER_ID)
      200.times { break if File.socket?(path); sleep 0.05 }
      raise "warm worker did not start" unless File.socket?(path)

      Minitest.after_run do
        Process.kill("TERM", pid)
        Process.wait(pid)
      end
      pid
    end
  end

  def setup
    super
    self.class.worker_pid
    @executor = SolidJobsGenericExecutor.new
  end

  def call(target, *arguments)
    @executor.execute_task(WORKER_ID, target, arguments)
  end

  def test_mutable_arguments_are_mutated_inside_worker_only
    items = ["a"]
    options = { "k" => 1 }
    result = call("WarmMutexJob", items, options)

    assert_equal %w[a mutated], result["items"]
    assert_equal({ "k" => 1, "seen" => true }, result["options"])
    assert_equal ["a"], items
    refute_equal Process.pid, result["pid"]
  end

  def test_thread_using_job
    assert_equal [2, 4, 6], call("WarmThreadJob", [1, 2, 3])
  end

  def test_fiber_using_job
    assert_equal [0, 1, 2, "done"], call("WarmFiberJob", 3)
  end

  def test_pool_holding_job
    assert_equal "pooled:x", call("WarmPoolJob", "x")
  end

  def test_remote_exception_is_raised
    error = assert_raises(SolidJobsGenericExecutor::RemoteError) { call("WarmBoomJob") }
    assert_match(/ArgumentError: kaboom/, error.message)
  end

  def test_unknown_class_is_reported_and_worker_survives
    assert_raises(SolidJobsGenericExecutor::RemoteError) { call("NopeNotAClass") }
    assert_equal 1, call("WarmPlainJob", 1)
  end

  def test_concurrent_clients
    results = Array.new(20) { |i| Thread.new { call("WarmThreadJob", [i]) } }.map(&:value)

    assert_equal(Array.new(20) { |i| [i * 2] }, results)
  end

  def test_requires_warm_pool_detection
    mutable = [[1]]

    assert WarmMutexJob.requires_warm_pool?(mutable)
    assert WarmThreadJob.requires_warm_pool?(mutable)
    assert WarmFiberJob.requires_warm_pool?(mutable)
    assert WarmPoolJob.requires_warm_pool?(mutable)
    refute WarmPlainJob.requires_warm_pool?(mutable)
    refute WarmMutexJob.requires_warm_pool?(Ractor.make_shareable([1]))
  end

  def test_enqueue_routes_to_generic_executor_or_stays_native
    WarmThreadJob.enqueue([1, 2])
    WarmPlainJob.enqueue([1, 2])

    routed = SolidJobsGenericExecutor.captured.fetch(0)
    assert_equal "SolidJobsGenericExecutor", routed["task"]
    assert_equal ["WarmThreadJob", [[1, 2]]], routed["arguments"].drop(1)
    assert_equal 1, WarmPlainJob.captured.size
  end

  def raw_request(line)
    UNIXSocket.open(format(SolidJobsGenericExecutor::SOCKET_PATH, WORKER_ID)) do |socket|
      socket.write(line)
      socket.close_write
      JSON.parse(socket.gets)
    end
  end

  def test_nested_structures_unicode_and_nil_round_trip
    payload = { "list" => [1, 2.5, nil, true], "text" => "héllo 日本語 🚀", "deep" => { "a" => { "b" => [[]] } } }

    assert_equal [payload, nil, "x"], call("WarmEchoJob", payload, nil, "x")
  end

  def test_symbol_keys_are_stringified_by_json
    assert_equal [{ "a" => 1 }], call("WarmEchoJob", { a: 1 })
  end

  def test_large_payload
    big = Array.new(20_000) { |i| "item-#{i}" }

    assert_equal big.size, call("WarmEchoJob", big).first.size
  end

  def test_namespaced_target_class
    assert_equal "nested:1", call("WarmNamespace::NestedJob", 1)
  end

  def test_malformed_json_returns_error_and_worker_survives
    reply = raw_request("not json\n")

    assert_equal "error", reply["status"]
    assert_equal 2, call("WarmEchoJob", 2).first
  end

  def test_missing_keys_return_error
    assert_equal "error", raw_request("{}\n")["status"]
  end

  def test_each_reply_is_a_single_json_line
    reply = raw_request(JSON.generate("target_class" => "WarmEchoJob", "arguments" => [1]) + "\n")

    assert_equal({ "status" => "ok", "result" => [1] }, reply)
  end

  def test_missing_worker_socket_raises
    assert_raises(Errno::ENOENT) { @executor.execute_task("does-not-exist", "WarmEchoJob", []) }
  end

  def test_slow_jobs_do_not_corrupt_other_replies_sequentially
    assert_kind_of Integer, call("WarmSlowJob", 0.2)
    assert_equal ["after"], call("WarmEchoJob", "after")
  end

  def test_repeated_calls_stay_in_same_warm_process
    pids = Array.new(5) { call("WarmMutexJob", [], {})["pid"] }

    assert_equal 1, pids.uniq.size
  end

  def test_worker_id_for_is_deterministic_and_in_range
    ids = Array.new(3) { SolidJobsGenericExecutor.worker_id_for("WarmMutexJob") }

    assert_equal 1, ids.uniq.size
    assert_includes 0...SolidJobsGenericExecutor.worker_count, ids.first
  end

  def test_shareable_arguments_stay_native_even_with_mutex
    WarmEchoJob.enqueue(1, "frozen")

    assert_equal 1, WarmEchoJob.captured.size
    assert_empty SolidJobsGenericExecutor.captured
  end

  def test_generic_executor_is_never_rerouted
    refute SolidJobsGenericExecutor.requires_warm_pool?([[1]])
  end

  def test_without_rails_nothing_is_routed
    Object.send(:remove_const, :Rails)
    refute WarmEchoJob.requires_warm_pool?([[1]])
  ensure
    Object.const_set(:Rails, Module.new)
  end

  def test_routed_payload_is_frozen_and_deep_copied
    original = [["a"]]
    WarmEchoJob.enqueue(*original)
    routed_arguments = SolidJobsGenericExecutor.captured.fetch(0)["arguments"]

    assert Ractor.shareable?(routed_arguments.last)
    refute_same original.first, routed_arguments.last.first
  end
end
