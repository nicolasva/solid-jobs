# frozen_string_literal: true

require "json"

module SolidJobs
  module Backends
    # Claim/execute/ack loop for channels served by a non-Redis backend
    # (SolidEngine). One instance per thread; it owns its backend connection.
    class Worker
      IDLE_SLEEP = 0.02

      attr_reader :processed, :failed

      def initialize(config:, backend_name:, channels:, processor_id: 0)
        @config = config
        @backend = config.build_backend(backend_name)
        @channels = channels.flat_map { |name, weight| Array.new(weight, name) }
        @processor_id = processor_id
        @executor = Executor.new(redis_config: config.redis_config, config: config)
        @processed = 0
        @failed = 0
        @cursor = 0
      end

      def run(stop:, paused: -> { false })
        until stop.call
          if paused.call
            sleep 0.05
            next
          end

          claim = next_claim
          if claim
            perform(claim)
          else
            sleep IDLE_SLEEP
          end
        end
        self
      ensure
        @backend.close
      end

      private

      def next_claim
        @channels.length.times do
          channel = @channels[@cursor % @channels.length]
          @cursor += 1
          claim = @backend.claim(channel)
          return claim if claim
        end
        nil
      rescue SystemCallError, IOError, EOFError, Backends::SolidEngine::ProtocolError => error
        @config.logger.warn("SolidJobs backend claim failed, retrying: #{error.class}: #{error.message}")
        sleep 0.1
        nil
      end

      def perform(claim)
        envelope = claim.envelope
        attributes = {reservation_id: claim.reservation_id, attempt: claim.attempt,
                      **Instrumentation.worker_attributes(@processor_id)}
        Instrumentation.emit(@config, :reserved, envelope, **attributes)
        Instrumentation.emit(@config, :started, envelope, **attributes)
        started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        keepalive = start_keepalive(claim)
        begin
          @executor.execute(envelope)
        rescue ExecutionHalt
          raise
        rescue StandardError => error
          duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started
          Instrumentation.emit(@config, :failed, envelope, **attributes, duration: duration,
                               error_class: error.class.name, error_message: error.message.to_s)
          handle_failure(claim, envelope, error, attributes)
          @failed += 1
        else
          duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started
          Instrumentation.emit(@config, :completed, envelope, **attributes, duration: duration)
          settle(claim) { @backend.ack(claim) }
          Instrumentation.emit(@config, :acknowledged, envelope, **attributes)
          @processed += 1
        end
      ensure
        stop_keepalive(keepalive)
      end

      # Mirrors FailurePolicy: retry with backoff while allowed, then discard.
      def handle_failure(claim, envelope, error, attributes)
        failure_count = claim.attempt
        task = resolve_task(envelope)
        decision = task.respond_to?(:retry_delay_callback) ? task.retry_delay_callback(failure_count, error, envelope) : nil
        if retry_allowed?(envelope, claim.attempt) && !%i[drop archive].include?(decision)
          delay = decision.is_a?(Numeric) ? decision : backoff(failure_count)
          settle(claim) { @backend.retry_job(claim, delay: delay) }
          Instrumentation.emit(@config, :retry_scheduled, envelope, **attributes,
                               error_class: error.class.name, error_message: error.message.to_s)
        else
          task.final_failure_callback(envelope, error) if decision != :drop && task.respond_to?(:final_failure_callback)
          settle(claim) { @backend.fail_job(claim) }
          Instrumentation.emit(@config, :dead, envelope, **attributes,
                               error_class: error.class.name, error_message: error.message.to_s)
        end
      end

      def retry_allowed?(envelope, attempt)
        if envelope["retry_within"]
          created = Float(envelope.fetch("created_ms", Utilities.realtime_milliseconds)) / 1_000
          Time.now.to_f - created < Float(envelope["retry_within"])
        else
          attempt <= Integer(envelope.fetch("max_failures", 0))
        end
      end

      def backoff(failure_count)
        ceiling = [Float(@config.retry_base_delay) * (2**(failure_count - 1)), Float(@config.retry_max_delay)].min
        (ceiling / 2) + (rand * ceiling / 2)
      end

      def resolve_task(envelope)
        Utilities.constantize(envelope.fetch("task"))
      rescue NameError
        nil
      end

      # A stale reservation means the job was reclaimed: fencing did its job.
      def settle(claim)
        yield
      rescue Backends::SolidEngine::Error => error
        raise unless error.code == :stale_reservation

        @config.logger.warn("Claim fencing rejected stale settlement: #{claim.id}")
      end

      def start_keepalive(claim)
        return unless @backend.supports?(:lease_extension)

        interval = @backend.lease_ms / 3000.0
        done = Queue.new
        thread = Thread.new do
          until done.pop(timeout: interval)
            begin
              @backend.extend_lease(claim)
            rescue StandardError
              break
            end
          end
        end
        [thread, done]
      end

      # Joined rather than killed: a killed thread could leave an unread reply on the socket.
      def stop_keepalive(keepalive)
        return unless keepalive

        thread, done = keepalive
        done << true
        thread.join
      end
    end
  end
end
