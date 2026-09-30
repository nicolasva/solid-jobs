# frozen_string_literal: true

require "securerandom"
require "socket"

module SolidJobs
  class Server
    attr_reader :config, :identity, :ractors

    def initialize(config: SolidJobs.config)
      @config = config
      @ractors = []
      @scheduler = nil
      @heartbeat = nil
      @started = false
      @identity = "#{Socket.gethostname}:#{::Process.pid}:#{SecureRandom.hex(6)}".freeze
      @started_at = Time.now.to_f
    end

    def start
      raise Error, "Server is already running" if @started

      snapshot = Utilities.shareable_copy(
        config.ractor_snapshot.merge(identity: identity, started_at: @started_at),
      )
      Recovery.call(config: config)
      barrier = StartupBarrier.new(config.concurrency + 2)
      heartbeat_ready = barrier.channel
      @heartbeat = Ractor.new(snapshot, heartbeat_ready) do |settings, ready|
        local_config = heartbeat = nil
        begin
          started = SolidJobs::StartupBarrier.boot(ready) do
            local_config = SolidJobs::Config.from_ractor_snapshot(settings)
            SolidJobs.use_config(local_config)
            heartbeat = SolidJobs::Heartbeat.new(
              local_config,
              identity: settings.fetch(:identity),
              started_at: settings.fetch(:started_at),
              concurrency: settings.fetch(:concurrency),
            )
            local_config.redis_pool.call("PING")
          end
          next :aborted unless started
          messages = ::Queue.new
          listener = Thread.new do
            loop do
              message = Ractor.receive
              messages << message
              break if message == :stop
            end
          end
          quiet = false
          stopping = false
          work = {}
          processed = failed = 0
          next_beat = 0.0
          until stopping
            begin
              until messages.empty?
                case (message = messages.pop(true))
                when :quiet
                  quiet = true
                when :stop
                  stopping = true
                else
                  event, *arguments = message
                  case event
                  when :work
                    work[arguments[0].to_s] = arguments[1]
                  when :done
                    work.delete(arguments[0].to_s)
                  when :stats
                    processed += arguments[0]
                    failed += arguments[1]
                  end
                end
              end
            rescue ThreadError
              nil
            end
            now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
            if now >= next_beat
              begin
                heartbeat.beat(quiet: quiet, work: work, processed: processed, failed: failed)
                processed = failed = 0
              rescue SolidRedis::ConnectionError => error
                local_config.logger.warn("Redis heartbeat failed, retrying: #{error.class}: #{error.message}")
              end
              next_beat = now + SolidJobs::Heartbeat::INTERVAL
            end
            sleep 0.05 unless stopping
          end
          begin
            heartbeat.beat(quiet: quiet, work: work, processed: processed, failed: failed)
            heartbeat.cleanup
          rescue SolidRedis::ConnectionError => error
            local_config.logger.warn("Redis heartbeat cleanup failed: #{error.class}: #{error.message}")
          end
          listener.join
          :stopped
        ensure
          local_config&.close
        end
      end
      barrier.register!(@heartbeat, heartbeat_ready)
      @ractors = Array.new(config.concurrency) do |processor_id|
        ready = barrier.channel
        ractor = Ractor.new(
          snapshot, processor_id, @heartbeat, ready,
        ) do |settings, id, heartbeat_port, ready_port|
          local_config = processor = nil
          begin
            started = SolidJobs::StartupBarrier.boot(ready_port) do
              local_config = SolidJobs::Config.from_ractor_snapshot(settings)
              SolidJobs.use_config(local_config)
              processor = SolidJobs::Processor.new(
                local_config,
                identity: settings.fetch(:identity),
                processor_id: id,
                heartbeat: heartbeat_port,
              )
              local_config.redis_pool.call("PING")
            end
            next :aborted unless started
            control = ::Queue.new
            listener = Thread.new do
              loop do
                command = Ractor.receive
                control << command
                next unless command == :stop

                if processor.busy?
                  sleep settings.fetch(:shutdown_timeout)
                  processor.interrupt_current
                end
                break
              end
            end
            result = processor.run(control: control)
            listener.join
            result
          ensure
            local_config&.close
          end
        end
        barrier.register!(ractor, ready)
        ractor
      end
      scheduler_ready = barrier.channel
      @scheduler = Ractor.new(
        snapshot, scheduler_ready,
      ) do |settings, ready|
        local_config = scheduler = nil
        begin
          started = SolidJobs::StartupBarrier.boot(ready) do
            local_config = SolidJobs::Config.from_ractor_snapshot(settings)
            SolidJobs.use_config(local_config)
            scheduler = SolidJobs::Scheduler.new(local_config)
            local_config.redis_pool.call("PING")
          end
          next :aborted unless started
          control = ::Queue.new
          listener = Thread.new do
            command = Ractor.receive
            control << command
          end
          scheduler.run(stop: -> { !control.empty? })
          listener.join
          :stopped
        ensure
          local_config&.close
        end
      end
      barrier.register!(@scheduler, scheduler_ready)
      barrier.run!
      @started = true
      config.fire(:startup, self)
      self
    rescue StandardError
      barrier&.abort!
      @ractors = []
      @scheduler = @heartbeat = nil
      raise
    end

    def quiet
      return self unless @started

      @ractors.each { |ractor| ractor.send(:quiet) }
      @heartbeat&.send(:quiet)
      config.fire(:quiet, self)
      self
    end

    def stop
      return [] unless @started

      @ractors.each { |ractor| ractor.send(:stop) }
      @scheduler&.send(:stop)
      results = @ractors.map { |ractor| RactorSupport.value(ractor) }
      RactorSupport.value(@scheduler) if @scheduler
      @heartbeat&.send(:stop)
      RactorSupport.value(@heartbeat) if @heartbeat
      @ractors = []
      @scheduler = nil
      @heartbeat = nil
      @started = false
      config.fire(:shutdown, self)
      results
    end

    def running?
      @started
    end

    def remote_signal
      config.redis_pool.call("RPOP", "#{identity}-signals")
    end

  end
end
