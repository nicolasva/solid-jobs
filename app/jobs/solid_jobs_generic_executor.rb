# frozen_string_literal: true

require "json"
require "socket"
require "zlib"
require "solid_jobs"

# Universal router: forwards a task to a persistent warm worker over a Unix socket.
class SolidJobsGenericExecutor
  include SolidJobs::Task

  class RemoteError < StandardError; end
  class TimeoutError < RemoteError; end

  SOCKET_PATH = ENV.fetch("SOLID_JOBS_WARM_SOCKET", "/tmp/solid_jobs_rails_%s.sock").freeze
  TIMEOUT = Float(ENV.fetch("SOLID_JOBS_WARM_TIMEOUT", "30")).freeze
  ROUNDTRIP_EVENT = "warm_pool.roundtrip"

  def self.worker_count
    Integer(ENV.fetch("SOLID_JOBS_WARM_WORKERS", 1))
  end

  def self.worker_id_for(task_name)
    Zlib.crc32(task_name.to_s) % worker_count
  end

  def execute_task(worker_id, target_class, arguments_array)
    payload = JSON.generate("target_class" => target_class, "arguments" => arguments_array)
    status = "error"
    started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

    response = UNIXSocket.open(format(SOCKET_PATH, worker_id)) do |socket|
      socket.write("#{payload}\n")
      socket.close_write
      unless socket.wait_readable(TIMEOUT)
        status = "timeout"
        raise TimeoutError, "Warm worker #{worker_id} did not answer within #{TIMEOUT}s"
      end

      socket.gets
    end
    raise RemoteError, "Warm worker #{worker_id} closed the connection without answering" if response.nil?

    reply = JSON.parse(response)
    if reply["status"] == "error"
      raise RemoteError, "#{reply["error_class"]}: #{reply["message"]}"
    end

    status = "ok"
    reply["result"]
  rescue Errno::ECONNREFUSED, Errno::EAGAIN
    # Listen backlog full (every warm worker busy) or worker down.
    status = "refused"
    raise
  ensure
    SolidJobs::Instrumentation.observe(
      SolidJobs.config,
      ROUNDTRIP_EVENT,
      worker_id: worker_id,
      status: status,
      duration: ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started,
    )
  end
end
