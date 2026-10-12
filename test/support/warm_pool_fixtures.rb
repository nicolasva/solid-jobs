# frozen_string_literal: true

# Loaded by the warm worker subprocess (-r) and by the tests.
class WarmMutexJob
  include SolidJobs::Task
  @lock = Mutex.new

  def execute_task(items, options)
    items << "mutated"
    { "items" => items, "options" => options.merge("seen" => true), "pid" => Process.pid }
  end
end

class WarmThreadJob
  include SolidJobs::Task
  @runner = Thread.new { sleep }

  def execute_task(numbers)
    numbers.map { |n| Thread.new { n * 2 } }.map(&:value)
  end
end

class WarmFiberJob
  include SolidJobs::Task
  @fiber = Fiber.new { :idle }

  def execute_task(count)
    fiber = Fiber.new { count.times { |i| Fiber.yield i }; :done }
    Array.new(count + 1) { fiber.resume }
  end
end

class WarmConnectionPool
end

class WarmPoolJob
  include SolidJobs::Task
  @connections = WarmConnectionPool.new

  def execute_task(value) = "pooled:#{value}"
end

class WarmPlainJob
  include SolidJobs::Task

  def execute_task(value) = value
end

class WarmBoomJob
  include SolidJobs::Task
  @lock = Mutex.new

  def execute_task = raise(ArgumentError, "kaboom")
end

class WarmEchoJob
  include SolidJobs::Task
  @lock = Mutex.new

  def execute_task(*arguments) = arguments
end

class WarmSlowJob
  include SolidJobs::Task
  @lock = Mutex.new

  def execute_task(seconds)
    sleep seconds
    Process.pid
  end
end

module WarmNamespace
  class NestedJob
    include SolidJobs::Task
    @lock = Mutex.new

    def execute_task(value) = "nested:#{value}"
  end
end
