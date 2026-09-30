# frozen_string_literal: true

require_relative "test_helper"

class RecorderMiddleware
  def initialize(events, name)
    @events = events
    @name = name
  end

  def call(*)
    @events << :"before_#{@name}"
    yield.tap { @events << :"after_#{@name}" }
  end
end

class FirstRecorderMiddleware < RecorderMiddleware; end
class SecondRecorderMiddleware < RecorderMiddleware; end

class MiddlewareTest < Minitest::Test
  def test_chain_nests_entries
    events = []
    chain = SolidJobs::Middleware::Chain.new
    chain.add(FirstRecorderMiddleware, events, :one)
    chain.add(SecondRecorderMiddleware, events, :two)

    result = chain.invoke(:job) do
      events << :job
      :result
    end

    assert_equal :result, result
    assert_equal %i[before_one before_two job after_two after_one], events
  end

  def test_nil_client_middleware_suppresses_job
    middleware = Class.new do
      def call(*)
        nil
      end
    end
    SolidJobs.config.client_middleware.add(middleware)

    assert_nil HardJob.perform_async(1)
    assert_empty HardJob.jobs
  end
end
