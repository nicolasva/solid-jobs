# frozen_string_literal: true

require "fileutils"
require "socket"
require "tmpdir"

module RedisTestServer
  module_function

  def config
    start
    SolidRedis::Config.new(
      host: "127.0.0.1",
      port: @port,
      timeout: 0.25,
      reconnect_attempts: 2,
    )
  end

  def start
    return if @pid

    @directory = Dir.mktmpdir("solid-jobs-redis-")
    @port ||= reserve_port
    start_process
    self
  rescue StandardError
    stop
    raise
  end

  def restart
    terminate_process
    start_process
    self
  end

  def interrupt
    terminate_process("KILL")
    self
  end

  def start_process
    @pid = Process.spawn(
      "redis-server",
      "--bind", "127.0.0.1",
      "--port", @port.to_s,
      "--save", "",
      "--appendonly", "no",
      "--dir", @directory,
      out: File::NULL,
      err: File::NULL,
    )
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
    loop do
      begin
        socket = TCPSocket.new("127.0.0.1", @port)
        socket.close
        break
      rescue Errno::ECONNREFUSED
        raise "Redis test server did not start" if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline

        sleep 0.01
      end
    end
  end

  def stop
    terminate_process
    FileUtils.remove_entry(@directory) if File.directory?(@directory)
    @pid = @directory = @port = nil
  end

  def terminate_process(signal = "TERM")
    return unless @pid

    Process.kill(signal, @pid)
    Process.wait(@pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  ensure
    @pid = nil
  end

  def reserve_port
    server = TCPServer.new("127.0.0.1", 0)
    server.local_address.ip_port
  ensure
    server&.close
  end
end

Minitest.after_run { RedisTestServer.stop }
