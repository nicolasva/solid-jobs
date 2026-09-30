# frozen_string_literal: true

require "fileutils"
require "socket"
require "tmpdir"

class CpuScalingRedis
  attr_reader :url

  def start
    @directory = Dir.mktmpdir("solid-jobs-cpu-scaling-")
    socket = TCPServer.new("127.0.0.1", 0)
    port = socket.local_address.ip_port
    socket.close
    @url = "redis://127.0.0.1:#{port}/0"
    @pid = Process.spawn(
      "redis-server",
      "--bind", "127.0.0.1",
      "--port", port.to_s,
      "--protected-mode", "no",
      "--save", "",
      "--appendonly", "no",
      "--dir", @directory,
      out: File::NULL,
      err: File::NULL,
    )
    wait_until_ready(port)
    self
  rescue StandardError
    close
    raise
  end

  def close
    if @pid
      Process.kill("TERM", @pid)
      Process.wait(@pid)
    end
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  ensure
    FileUtils.remove_entry(@directory) if @directory && File.directory?(@directory)
    @pid = @directory = nil
  end

  private

  def wait_until_ready(port)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
    loop do
      socket = TCPSocket.new("127.0.0.1", port)
      socket.close
      return
    rescue Errno::ECONNREFUSED
      raise "Redis did not start" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

      sleep 0.01
    end
  end
end
