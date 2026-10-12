#!/usr/bin/env ruby
# frozen_string_literal: true

# Persistent IPC worker, run outside any Ractor (Rails booted once).
# Usage: ruby script/warm_worker.rb WORKER_ID

require "json"
require "socket"

worker_id = ARGV.fetch(0) { abort "Usage: #{$PROGRAM_NAME} WORKER_ID" }
abort "WORKER_ID must be alphanumeric" unless worker_id.match?(/\A\w+\z/)

path = format(ENV.fetch("SOLID_JOBS_WARM_SOCKET", "/tmp/solid_jobs_rails_%s.sock"), worker_id)
File.delete(path) if File.exist?(path)

env_file = File.expand_path("config/environment.rb", Dir.pwd)
require env_file if File.exist?(env_file)

server = UNIXServer.new(path)
File.chmod(0o600, path)
shutdown = false
%w[INT TERM].each { |signal| trap(signal) { shutdown = true; server.close } }

until shutdown
  begin
    socket = server.accept
  rescue IOError, Errno::EBADF
    break
  end

  begin
    request = JSON.parse(socket.gets.to_s)
    target = Object.const_get(request.fetch("target_class"))
    arguments = request.fetch("arguments")
    instance = target.respond_to?(:new) ? target.new : target
    method = instance.respond_to?(:execute_task) ? :execute_task : :perform
    reply = { "status" => "ok", "result" => instance.public_send(method, *arguments) }
  rescue Exception => e # rubocop:disable Lint/RescueException
    reply = { "status" => "error", "error_class" => e.class.name, "message" => e.message }
  end

  begin
    socket.write("#{JSON.generate(reply)}\n")
  rescue StandardError
    nil
  ensure
    socket.close
  end
end

File.delete(path) if File.exist?(path)
