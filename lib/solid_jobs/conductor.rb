# frozen_string_literal: true

require "securerandom"
require "socket"
require "timeout"

module SolidJobs
  class Conductor
    # Extra time granted beyond shutdown_timeout before a component is
    # considered stuck, and how many times :stop is re-sent before giving up.
    STOP_GRACE = 5.0
    STOP_RESENDS = 3

    attr_reader :config, :identity, :ractors

    def initialize(config: SolidJobs.config, stop_grace: STOP_GRACE)
      @config = config
      @stop_grace = stop_grace
      @ractors = []
      @scheduler = nil
      @heartbeat = nil
      @started = false
      @identity = "#{Socket.gethostname}:#{::Process.pid}:#{SecureRandom.hex(6)}".freeze
      @started_at = Time.now.to_f
    end

    def start
      raise Error, "Conductor is already running" if @started

      warn_ruby34_multi_ractor
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
            local_config = SolidJobs::Blueprint.from_ractor_snapshot(settings)
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
              local_config = SolidJobs::Blueprint.from_ractor_snapshot(settings)
              SolidJobs.use_config(local_config)
              processor = SolidJobs::Engine.new(
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
            local_config = SolidJobs::Blueprint.from_ractor_snapshot(settings)
            SolidJobs.use_config(local_config)
            scheduler = SolidJobs::Timer.new(local_config)
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
      results = @ractors.each_with_index.map do |ractor, index|
        await_termination(ractor, "processor #{index}")
      end
      await_termination(@scheduler, "scheduler") if @scheduler
      @heartbeat&.send(:stop)
      await_termination(@heartbeat, "heartbeat") if @heartbeat
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

    private

    RUBY34_MULTI_RACTOR_WARNING =
      "SolidJobs: Ruby %s can deadlock the whole VM on a Ractor GC barrier " \
      "under multi-Engine load (see docs/reliability.md). Run multi-Ractor " \
      "servers on Ruby >= 4.0, or use concurrency: 1 (one process per Engine)."

    def warn_ruby34_multi_ractor
      return if RUBY_VERSION >= "4" || config.concurrency <= 1

      config.logger.warn(format(RUBY34_MULTI_RACTOR_WARNING, RUBY_VERSION))
    end

    # Waits for a component Ractor to return after :stop. Ruby 3.4 can lose
    # the wakeup of a Ractor.receive running in a secondary thread while the
    # inbox is busy; a fresh :stop message re-triggers it. If the component
    # still does not return, it is abandoned so shutdown never hangs forever.
    def await_termination(ractor, name)
      deadline = config.shutdown_timeout + @stop_grace
      resends = 0
      begin
        Timeout.timeout(deadline) { RactorSupport.value(ractor) }
      rescue Timeout::Error
        if resends < STOP_RESENDS
          resends += 1
          config.logger.warn(
            "SolidJobs #{name} did not stop within #{deadline}s, " \
            "re-sending :stop (#{resends}/#{STOP_RESENDS})",
          )
          begin
            ractor.send(:stop)
          rescue Ractor::ClosedError
            nil
          end
          retry
        end
        config.logger.error("SolidJobs #{name} did not stop; abandoning it")
        nil
      end
    end

    def remote_signal
      config.redis_pool.call("RPOP", Keyspace.node_signals(identity))
    end

  end
end
