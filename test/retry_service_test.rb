# frozen_string_literal: true

require_relative "test_helper"

class RetryServiceTest < Minitest::Test
  def test_exponential_backoff_uses_equal_jitter
    service = SolidJobs::FailurePolicy.new(
      config: SolidJobs.config,
      payload: {},
      error: RuntimeError.new,
    )
    delays = Array.new(10_000) { service.send(:retry_delay, 1) }

    assert_operator delays.min, :>=, 7.5
    assert_operator delays.max, :<=, 15.0
    assert_operator delays.map { |delay| delay.to_i }.uniq.size, :>=, 8
  end

  def test_exponential_backoff_is_capped
    config = SolidJobs.config
    config.retry_max_delay = 60
    service = SolidJobs::FailurePolicy.new(
      config: config,
      payload: {},
      error: RuntimeError.new,
    )
    delays = Array.new(1_000) { service.send(:retry_delay, 20) }

    assert_operator delays.min, :>=, 30
    assert_operator delays.max, :<=, 60
  end
end
