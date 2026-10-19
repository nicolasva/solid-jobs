# frozen_string_literal: true

# Worker subprocess for the reliability tests: serves channel "rel" from
# SolidEngine until it receives TERM (clean stop) or is killed (crash).
require "solid_jobs"
require_relative "reliability_task"

url = ENV.fetch("RELIABILITY_URL")
config = SolidJobs::Blueprint.new(channels: %w[rel])
config.logger = Logger.new(File::NULL)
config.backend = :solid_engine
config.backends = {solid_engine: {adapter: :solid_engine, url: url,
                                  lease_ms: Integer(ENV.fetch("RELIABILITY_LEASE_MS", "1500"))}}
config.execution_mode = ENV.fetch("RELIABILITY_MODE", "thread").to_sym
config.concurrency = Integer(ENV.fetch("RELIABILITY_CONCURRENCY", "1"))
SolidJobs.use_config(config)

conductor = SolidJobs::Conductor.new(config: config).start
stopping = Queue.new
Signal.trap("TERM") { stopping << true }
puts "READY"
$stdout.flush
stopping.pop
conductor.stop
