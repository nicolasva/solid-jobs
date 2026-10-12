# frozen_string_literal: true

require "json"
require "socket"
require "solid_jobs"

# Universal router: forwards a task to a persistent warm worker over a Unix socket.
class SolidJobsGenericExecutor
  include SolidJobs::Task

  class RemoteError < StandardError; end

  SOCKET_PATH = "/tmp/solid_jobs_rails_%s.sock"

  def self.worker_count
    Integer(ENV.fetch("SOLID_JOBS_WARM_WORKERS", 1))
  end

  def self.worker_id_for(task_name)
    task_name.to_s.hash.abs % worker_count
  end

  def execute_task(worker_id, target_class, arguments_array)
    payload = JSON.generate("target_class" => target_class, "arguments" => arguments_array)

    response = UNIXSocket.open(format(SOCKET_PATH, worker_id)) do |socket|
      socket.write("#{payload}\n")
      socket.close_write
      socket.gets
    end
    raise RemoteError, "Warm worker #{worker_id} closed the connection without answering" if response.nil?

    reply = JSON.parse(response)
    if reply["status"] == "error"
      raise RemoteError, "#{reply["error_class"]}: #{reply["message"]}"
    end

    reply["result"]
  end
end
