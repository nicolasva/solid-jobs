# frozen_string_literal: true

require_relative "test_helper"

class RecordingInterceptor
  def initialize(events, name)
    @events = events
    @name = name
  end

  def around(_context)
    @events << :"before_#{@name}"
    yield.tap { @events << :"after_#{@name}" }
  end
end

class FirstRecordingInterceptor < RecordingInterceptor; end
class SecondRecordingInterceptor < RecordingInterceptor; end

class InterceptorRegistryTest < Minitest::Test
  def test_interceptors_wrap_the_operation_in_registration_order
    events = []
    registry = SolidJobs::InterceptorRegistry.new
    registry.use(FirstRecordingInterceptor, events, :one)
    registry.use(SecondRecordingInterceptor, events, :two)

    result = registry.call(:context) do
      events << :operation
      :result
    end

    assert_equal :result, result
    assert_equal %i[before_one before_two operation after_two after_one], events
  end

  def test_publish_interceptor_can_suppress_a_task
    interceptor = Class.new do
      def around(_context)
        nil
      end
    end
    SolidJobs.config.publish_interceptors.use(interceptor)

    assert_nil HardJob.enqueue(1)
    assert_empty HardJob.captured
  end
end
