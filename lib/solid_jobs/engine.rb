# frozen_string_literal: true

require "json"

module SolidJobs
  class Engine
    def initialize(config, identity: nil, processor_id: nil, heartbeat: nil)
      @config = config
      @claims = Claim.new(config, identity: identity, processor_id: processor_id)
      @executor = Executor.new(redis_config: config.redis_config, config: config)
      @identity = identity
      @processor_id = processor_id
      @heartbeat = heartbeat
      @state_mutex = Mutex.new
      @busy = false
    end

    def run(control:)
      @state_mutex.synchronize { @runner = Thread.current }
      processed = 0
      failed = 0
      @quiet = false
      @stopping = false
      loop do
        receive_commands(control)
        break if @stopping
        if @quiet
          sleep 0.05
          next
        end

        claim = retrieve
        next unless claim

        receive_commands(control)
        if @stopping || @quiet
          requeue(claim)
          break if @stopping

          next
        end

        begin
          @state_mutex.synchronize { @busy = true }
          envelope = claim.envelope
          register_work(claim, envelope)
          attributes = telemetry_attributes(claim)
          Instrumentation.emit(@config, :started, envelope, **attributes)
          started_at = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
          begin
            @executor.execute(envelope)
          rescue ExecutionHalt
            requeue(claim)
            break
          rescue StandardError => error
            duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started_at
            Instrumentation.emit(
              @config,
              :failed,
              envelope,
              **attributes,
              duration: duration,
              error_class: error.class.name,
              error_message: error.message.to_s,
            )
            FailurePolicy.call(
              config: @config,
              payload: envelope,
              error: error,
              telemetry_attributes: attributes,
            )
            complete(claim)
            failed += 1
          else
            duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started_at
            Instrumentation.emit(@config, :completed, envelope, **attributes, duration: duration)
            processed += 1 if complete(claim)
          end
        ensure
          @state_mutex.synchronize { @busy = false }
          clear_work
        end
      end
      {processed: processed, failed: failed}
    ensure
      send_heartbeat(:stats, processed || 0, failed || 0)
      @config.close
    end

    def busy?
      @state_mutex.synchronize { @busy }
    end

    def interrupt_current
      runner = @state_mutex.synchronize { @runner if @busy }
      runner&.raise(ExecutionHalt, "SolidJobs shutdown timeout exceeded")
    end

    private

    def complete(claim)
      completed = claim.complete
      unless completed
        @config.logger.warn(
          "Claim fencing rejected stale completion: #{claim.claim_token}",
        )
      end
      if completed
        Instrumentation.emit(
          @config,
          :acknowledged,
          claim.envelope,
          **telemetry_attributes(claim),
        )
      end
      completed
    rescue SolidRedis::ConnectionError => error
      @claims.connection_failed!
      @config.logger.warn(
        "Redis completion failed; claimed task will be replayed: #{error.class}: #{error.message}",
      )
      false
    end

    def requeue(claim)
      requeued = claim.requeue
      unless requeued
        @config.logger.warn(
          "Claim fencing rejected stale requeue: #{claim.claim_token}",
        )
      end
      requeued
    end

    def retrieve
      @claims.next
    rescue SolidRedis::ConnectionError => error
      @claims.connection_failed!
      @config.logger.warn("Redis claim failed, retrying: #{error.class}: #{error.message}")
      sleep 0.1
      nil
    end

    def register_work(claim, envelope)
      return unless @identity

      work = JSON.generate(
        "channel" => claim.channel,
        "envelope" => envelope,
        "started_at" => Time.now.to_f,
        "claim_token" => claim.claim_token,
        "attempt" => claim.attempt,
      )
      send_heartbeat(:work, @processor_id, work)
    end

    def clear_work
      return unless @identity

      send_heartbeat(:done, @processor_id)
    end

    def send_heartbeat(*message)
      @heartbeat&.send(message, move: true)
    end

    def receive_commands(control)
      until control.empty?
        case control.pop(true)
        when :quiet
          @quiet = true
        when :stop
          @stopping = true
        end
      end
    rescue ThreadError
      nil
    end

    def telemetry_attributes(claim)
      {
        reservation_id: claim.claim_token,
        attempt: claim.attempt,
        **Instrumentation.worker_attributes(@processor_id),
      }
    end
  end
end
