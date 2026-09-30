# frozen_string_literal: true

require "json"

module SolidJobs
  class Processor
    def initialize(config, identity: nil, processor_id: nil, heartbeat: nil)
      @config = config
      @fetcher = Fetch.new(config, identity: identity, processor_id: processor_id)
      @worker = Worker.new(redis_config: config.redis_config, config: config)
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

        unit = retrieve
        next unless unit

        receive_commands(control)
        if @stopping || @quiet
          requeue(unit)
          break if @stopping

          next
        end

        begin
          @state_mutex.synchronize { @busy = true }
          payload = unit.job
          register_work(unit, payload)
          begin
            @worker.perform(payload)
          rescue Shutdown
            requeue(unit)
            break
          rescue StandardError => error
            RetryService.call(config: @config, payload: payload, error: error)
            acknowledge(unit)
            failed += 1
          else
            processed += 1 if acknowledge(unit)
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
      runner&.raise(Shutdown, "SolidJobs shutdown timeout exceeded")
    end

    private

    def acknowledge(unit)
      acknowledged = unit.acknowledge
      unless acknowledged
        @config.logger.warn(
          "Reservation fencing rejected stale ACK: #{unit.reservation_id}",
        )
      end
      acknowledged
    rescue SolidRedis::ConnectionError => error
      @fetcher.connection_failed!
      @config.logger.warn(
        "Redis ACK failed; reserved job will be replayed: #{error.class}: #{error.message}",
      )
      false
    end

    def requeue(unit)
      requeued = unit.requeue
      unless requeued
        @config.logger.warn(
          "Reservation fencing rejected stale requeue: #{unit.reservation_id}",
        )
      end
      requeued
    end

    def retrieve
      @fetcher.retrieve
    rescue SolidRedis::ConnectionError => error
      @fetcher.connection_failed!
      @config.logger.warn("Redis fetch failed, retrying: #{error.class}: #{error.message}")
      sleep 0.1
      nil
    end

    def register_work(unit, payload)
      return unless @identity

      work = JSON.generate(
        "queue" => unit.queue,
        "payload" => payload,
        "run_at" => Time.now.to_f,
        "reservation_id" => unit.reservation_id,
        "attempt" => unit.attempt,
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
  end
end
