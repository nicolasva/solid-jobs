# frozen_string_literal: true

require "timeout"

module SolidJobs
  class StartupBarrier
    STATES = %i[booting all_ready running boot_failed].freeze
    Component = Struct.new(:ractor, :start_channel, keyword_init: true)

    attr_reader :expected_count, :ready_count, :state

    def initialize(expected_count, timeout: 10)
      @expected_count = Integer(expected_count)
      raise ArgumentError, "expected_count must be positive" unless @expected_count.positive?

      @timeout = Float(timeout)
      raise ArgumentError, "timeout must be positive" unless @timeout.positive?

      @components = []
      @registered = {}
      @ready_count = 0
      @state = :booting
      @cleaned = false
    end

    def channel
      ensure_state!(:booting)
      Ractor::Port.new if defined?(Ractor::Port)
    end

    def register!(ractor, channel)
      ensure_state!(:booting)
      identity = ractor.object_id
      raise Error, "Ractor announced READY more than once" if @registered.key?(identity)

      signal, start_channel = wait_for_signal(ractor, channel)
      unless signal == :ready
        RactorSupport.value(ractor)
        raise Error, "Ractor failed during boot"
      end

      @registered[identity] = true
      @components << Component.new(ractor: ractor, start_channel: start_channel)
      @ready_count += 1
      @state = :all_ready if @ready_count == expected_count
      ractor
    rescue Timeout::Error
      boot_failed!(Error.new("Ractor boot timed out after #{@timeout}s"))
    rescue StandardError => error
      boot_failed!(error)
    end

    def run!
      ensure_state!(:all_ready)
      @components.each { |component| signal(component, :start) }
      @state = :running
      @components.map(&:ractor)
    end

    def abort!
      return self if @cleaned || state == :running

      @state = :boot_failed
      @cleaned = true
      @components.each { |component| signal(component, :abort) }
      @components.each do |component|
        RactorSupport.value(component.ractor)
      rescue Ractor::RemoteError, Ractor::ClosedError
        nil
      end
      self
    end

    def self.boot(channel)
      announced = false
      yield
      command = if channel
        start_channel = Ractor::Port.new
        channel << [:ready, start_channel]
        announced = true
        start_channel.receive
      else
        Ractor.yield(:ready)
        announced = true
        Ractor.receive
      end
      return false if command == :abort
      raise Error, "Invalid startup command: #{command.inspect}" unless command == :start

      true
    rescue StandardError
      channel << [:boot_error, nil] if channel && !announced
      raise
    end

    private

    def boot_failed!(error)
      @state = :boot_failed
      abort!
      raise error
    end

    def wait_for_signal(ractor, channel)
      Timeout.timeout(@timeout) do
        channel ? channel.receive : [ractor.take, nil]
      end
    end

    def signal(component, command)
      if component.start_channel
        component.start_channel << command
      else
        component.ractor.send(command)
      end
    rescue Ractor::ClosedError
      nil
    end

    def ensure_state!(required)
      return if state == required

      raise Error, "StartupBarrier is #{state}, expected #{required}"
    end
  end
end
