# frozen_string_literal: true

# Task shared by the reliability tests and the worker subprocess. Every phase is
# appended to RELIABILITY_LOG so a test can reconstruct what happened per job.
class ReliabilityTask
  include SolidJobs::Task
  task_options channel: "rel", max_failures: 0

  def execute_task(index)
    log("start #{index}")
    sleep(Float(ENV.fetch("RELIABILITY_SLOW", "0"))) if index == Integer(ENV.fetch("RELIABILITY_SLOW_INDEX", "-1"))
    sleep(0.01)
    log("done #{index}")
  end

  private

  def log(line)
    File.open(ENV.fetch("RELIABILITY_LOG"), "a") do |file|
      file.flock(File::LOCK_EX)
      file.puts("#{Process.pid} #{line}")
    end
  end
end
