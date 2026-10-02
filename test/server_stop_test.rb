# frozen_string_literal: true

require_relative "test_helper"
require "stringio"

class ServerStopTest < Minitest::Test
  # A component whose listener never observes :stop must not hang shutdown:
  # the server re-sends :stop a bounded number of times, then abandons it.
  def test_stop_is_bounded_when_a_component_ignores_stop
    log = StringIO.new
    config = SolidJobs::Blueprint.new(concurrency: 0)
    config.shutdown_timeout = 0.05
    config.logger = Logger.new(log)
    server = SolidJobs::Conductor.new(config: config, stop_grace: 0.1)
    stuck = Ractor.new do
      loop { break if Ractor.receive == :never }
    end
    server.instance_variable_set(:@started, true)
    server.instance_variable_set(:@heartbeat, stuck)

    started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    results = server.stop
    elapsed = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started

    assert_equal [], results
    refute server.running?
    assert_operator elapsed, :<, 3.0
    assert_equal SolidJobs::Conductor::STOP_RESENDS, log.string.scan("re-sending :stop").size
    assert_includes log.string, "heartbeat did not stop; abandoning it"
  ensure
    stuck&.send(:never)
    SolidJobs::RactorSupport.value(stuck) if stuck
    config&.close
  end

  def test_stop_returns_component_results_when_they_terminate
    config = SolidJobs::Blueprint.new(concurrency: 0)
    config.shutdown_timeout = 0.05
    server = SolidJobs::Conductor.new(config: config)
    processor = Ractor.new { Ractor.receive; {processed: 1, failed: 0} }
    server.instance_variable_set(:@started, true)
    server.instance_variable_set(:@ractors, [processor])

    assert_equal [{processed: 1, failed: 0}], server.stop
  ensure
    config&.close
  end

  def test_multi_ractor_warning_depends_on_ruby_version_and_concurrency
    log = StringIO.new
    config = SolidJobs::Blueprint.new(concurrency: 4)
    config.logger = Logger.new(log)
    server = SolidJobs::Conductor.new(config: config)

    server.send(:warn_ruby34_multi_ractor)

    if RUBY_VERSION < "4"
      assert_includes log.string, "Run multi-Ractor servers on Ruby >= 4.0"
    else
      assert_empty log.string
    end

    log.truncate(0)
    config.concurrency = 1
    server.send(:warn_ruby34_multi_ractor)
    assert_empty log.string
  ensure
    config&.close
  end
end
