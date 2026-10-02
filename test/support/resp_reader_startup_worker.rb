# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path("../../lib", __dir__))

require "json"
require "socket"
require "solid_redis"
require "uri"

ractor_count = Integer(ARGV.fetch(0))
reader_iterations = Integer(ARGV.fetch(1))
mode = ARGV.fetch(2)
warm_main = ARGV.fetch(3, "true") == "true"
transport = ARGV.fetch(4, "reader")
redis_url = ARGV[5]&.freeze

raise ArgumentError, "mode must be simultaneous or serialized" unless %w[simultaneous serialized].include?(mode)
transports = %w[reader socket reader_tcp client]
raise ArgumentError, "transport must be one of #{transports.join(", ")}" unless transports.include?(transport)
raise ArgumentError, "Redis URL is required for TCP transports" if transport != "reader" && !redis_url

if warm_main
  if transport == "client"
    client = SolidRedis::Config.new(url: redis_url, timeout: 1).new_client
    raise "main client warmup failed" unless client.call("PING") == "PONG"
    client.close
  elsif transport == "reader"
    reader_socket, writer_socket = Socket.pair(:UNIX, :STREAM, 0)
    writer_socket.write("+READY\r\n")
    reader = SolidRedis::RESP::Reader.new(reader_socket, read_timeout: 1)
    raise "main Reader warmup failed" unless reader.read == "READY"
    reader_socket.close
    writer_socket.close
  else
    endpoint = URI(redis_url)
    socket = Socket.tcp(endpoint.host, endpoint.port, connect_timeout: 1)
    if transport == "reader_tcp"
      socket.write("*1\r\n$4\r\nPING\r\n")
      reader = SolidRedis::RESP::Reader.new(socket, read_timeout: 1)
      raise "main TCP Reader warmup failed" unless reader.read == "PONG"
    end
    socket.close
  end
end

def spawn_socket_ractor(start_at, reader_iterations, redis_url, with_reader)
  Ractor.new(start_at, reader_iterations, redis_url, with_reader) do |deadline, iterations, url, parse_response|
    endpoint = URI(url)
    remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sleep remaining if remaining.positive?
    iterations.times do
      socket = Socket.tcp(endpoint.host, endpoint.port, connect_timeout: 1)
      if parse_response
        socket.write("*1\r\n$4\r\nPING\r\n")
        reader = SolidRedis::RESP::Reader.new(socket, read_timeout: 1)
        raise "TCP Reader returned an invalid response" unless reader.read == "PONG"
      end
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, [1, 0].pack("ii"))
      socket.close
    end
    true
  end
end

if ENV["TRACE_STARTUP_REQUIRE"] == "1"
  module StartupRequireTrace
    def require(path)
      warn "require ractor=#{Ractor.current.object_id} path=#{path}"
      super
    end
  end
  Kernel.prepend(StartupRequireTrace)
end

def spawn_reader_ractor(start_at, reader_iterations)
  Ractor.new(start_at, reader_iterations) do |deadline, iterations|
    remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sleep remaining if remaining.positive?
    iterations.times do
      reader_socket, writer_socket = Socket.pair(:UNIX, :STREAM, 0)
      writer_socket.write("+OK\r\n")
      reader = SolidRedis::RESP::Reader.new(reader_socket, read_timeout: 1)
      raise "Reader returned an invalid response" unless reader.read == "OK"
      reader_socket.close
      writer_socket.close
    end
    true
  end
end

def spawn_client_ractor(start_at, reader_iterations, redis_url)
  Ractor.new(start_at, reader_iterations, redis_url) do |deadline, iterations, url|
    remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
    sleep remaining if remaining.positive?
    config = SolidRedis::Config.new(url: url, timeout: 1)
    iterations.times do
      client = config.new_client
      raise "client PING failed" unless client.call("PING") == "PONG"
      client.close
    end
    true
  end
end

spawn_worker = if transport == "client"
  ->(start_at) { spawn_client_ractor(start_at, reader_iterations, redis_url) }
elsif transport == "socket" || transport == "reader_tcp"
  with_reader = transport == "reader_tcp"
  ->(start_at) { spawn_socket_ractor(start_at, reader_iterations, redis_url, with_reader) }
else
  ->(start_at) { spawn_reader_ractor(start_at, reader_iterations) }
end

started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
if mode == "simultaneous"
  start_at = started + 0.1
  workers = Array.new(ractor_count) { spawn_worker.call(start_at) }
  workers.each { |worker| worker.respond_to?(:value) ? worker.value : worker.take }
else
  ractor_count.times do
    worker = spawn_worker.call(Process.clock_gettime(Process::CLOCK_MONOTONIC))
    worker.respond_to?(:value) ? worker.value : worker.take
  end
end

puts JSON.generate(
  ruby: RUBY_VERSION,
  ractors: ractor_count,
  readers_per_ractor: reader_iterations,
  mode: mode,
  warm_main: warm_main,
  transport: transport,
  elapsed: Process.clock_gettime(Process::CLOCK_MONOTONIC) - started,
)
