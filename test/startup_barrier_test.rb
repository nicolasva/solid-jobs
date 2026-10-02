# frozen_string_literal: true

require_relative "test_helper"

class StartupBarrierTest < Minitest::Test
  def test_releases_components_only_after_all_are_ready
    barrier = SolidJobs::StartupBarrier.new(2)
    first = spawn_component(barrier)
    barrier.register!(first.fetch(:ractor), first.fetch(:channel))

    assert_equal :booting, barrier.state
    assert_equal 1, barrier.ready_count

    second = spawn_component(barrier)
    barrier.register!(second.fetch(:ractor), second.fetch(:channel))

    assert_equal :all_ready, barrier.state
    assert_equal 2, barrier.ready_count

    barrier.run!

    assert_equal :running, barrier.state
    assert_raises(SolidJobs::Error) { barrier.run! }
    assert_same barrier, barrier.abort!
    assert_equal :running, barrier.state
    assert_equal :running, SolidJobs::RactorSupport.value(first.fetch(:ractor))
    assert_equal :running, SolidJobs::RactorSupport.value(second.fetch(:ractor))
  end

  def test_duplicate_ready_is_terminal
    barrier = SolidJobs::StartupBarrier.new(2)
    component = spawn_component(barrier)
    ractor = component.fetch(:ractor)
    channel = component.fetch(:channel)
    barrier.register!(ractor, channel)

    error = assert_raises(SolidJobs::Error) do
      barrier.register!(ractor, channel)
    end

    assert_match(/more than once/, error.message)
    assert_equal :boot_failed, barrier.state
    assert_raises(SolidJobs::Error) { barrier.run! }
  end

  def test_component_failure_aborts_previously_ready_components
    barrier = SolidJobs::StartupBarrier.new(3)
    first = spawn_component(barrier)
    barrier.register!(first.fetch(:ractor), first.fetch(:channel))
    failing = spawn_component(barrier, fail_boot: true)

    assert_raises(Ractor::RemoteError) do
      barrier.register!(failing.fetch(:ractor), failing.fetch(:channel))
    end

    assert_equal :boot_failed, barrier.state
    assert_equal 1, barrier.ready_count
    assert_raises(SolidJobs::Error) { barrier.run! }
  end

  def test_cleanup_is_idempotent
    barrier = SolidJobs::StartupBarrier.new(2)
    component = spawn_component(barrier)
    barrier.register!(component.fetch(:ractor), component.fetch(:channel))

    assert_same barrier, barrier.abort!
    assert_same barrier, barrier.abort!
    assert_equal :boot_failed, barrier.state
  end

  private

  def spawn_component(barrier, fail_boot: false)
    channel = barrier.channel
    ractor = Ractor.new(channel, fail_boot) do |ready, should_fail|
      Thread.current.report_on_exception = false
      started = SolidJobs::StartupBarrier.boot(ready) do
        raise "boot failure" if should_fail
      end
      next :aborted unless started

      :running
    end
    {ractor: ractor, channel: channel}
  end
end
