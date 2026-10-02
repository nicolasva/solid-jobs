# frozen_string_literal: true

require "optparse"
require "yaml"

module SolidJobs
  class CLI
    SIGNALS = %w[INT TERM TSTP TTIN INFO].freeze

    def initialize
      @options = {channels: []}
    end

    def run(arguments = ARGV)
      parse(arguments)
      load_configuration
      load_application
      apply_options
      server = Server.new(config: SolidJobs.config).start
      read_io, write_io = IO.pipe
      handlers = install_signal_handlers(write_io)
      wait_for_shutdown(server, read_io)
      server.stop
      0
    ensure
      handlers&.each { |signal, handler| Signal.trap(signal, handler) }
      read_io&.close
      write_io&.close
    end

    def parse(arguments)
      parser.parse!(arguments)
      @options
    end

    def parser
      @parser ||= OptionParser.new do |options|
        options.banner = "Usage: solid-jobs [options]"
        options.on("-r", "--require PATH", "Require an application file") { |value| @options[:require] = value }
        options.on("-C", "--config PATH", "Load YAML configuration") { |value| @options[:config] = value }
        options.on("-c", "--concurrency N", Integer, "Processor Ractor count") { |value| @options[:concurrency] = value }
        options.on("--channel CHANNEL", "Channel name or name,weight") do |value|
          @options[:channels] << channel(value)
        end
        options.on("-e", "--environment NAME", "Application environment") { |value| @options[:environment] = value }
        options.on("-t", "--timeout SECONDS", Float, "Graceful shutdown timeout") { |value| @options[:timeout] = value }
        options.on("-v", "--version", "Print version") do
          puts "solid-jobs #{VERSION}"
          exit
        end
        options.on("-h", "--help", "Print help") do
          puts options
          exit
        end
      end
    end

    private

    def load_configuration
      return unless @options[:config]

      values = YAML.safe_load_file(@options[:config], permitted_classes: [Symbol], aliases: true) || {}
      values = values.fetch(@options[:environment], values) if @options[:environment]
      @options[:concurrency] ||= values["concurrency"]
      @options[:timeout] ||= values["timeout"]
      if @options[:channels].empty?
        @options[:channels] = Array(values["channels"]).map { |value| channel(value) }
      end
      if (url = values.dig("redis", "url") || values["redis_url"])
        SolidJobs.config.redis = SolidRedis::Config.new(url: url)
      end
    end

    def load_application
      return unless @options[:require]

      require File.expand_path(@options[:require])
    end

    def apply_options
      ENV["RAILS_ENV"] = ENV["RACK_ENV"] = @options[:environment] if @options[:environment]
      SolidJobs.config.concurrency = @options[:concurrency] if @options[:concurrency]
      SolidJobs.config.channels = @options[:channels] unless @options[:channels].empty?
      SolidJobs.config.shutdown_timeout = @options[:timeout] if @options[:timeout]
    end

    def install_signal_handlers(write_io)
      SIGNALS.to_h do |signal|
        previous = Signal.trap(signal) do
          write_io.write_nonblock("#{signal}\n", exception: false)
        end
        [signal, previous]
      rescue ArgumentError
        [signal, "DEFAULT"]
      end
    end

    def wait_for_shutdown(server, read_io)
      loop do
        if IO.select([read_io], nil, nil, 0.5)
          signal = read_io.gets&.strip
          return if handle_signal(server, signal)
        end
        remote = server.remote_signal
        return if remote && handle_signal(server, remote)
      end
    end

    def handle_signal(server, signal)
      case signal
      when "INT", "TERM"
        true
      when "TSTP"
        server.quiet
        false
      when "TTIN", "INFO"
        dump_threads
        false
      else
        false
      end
    end

    def dump_threads
      Thread.list.each do |thread|
        SolidJobs.config.logger.info(
          "Thread #{thread.object_id}: #{Array(thread.backtrace).join("\n")}",
        )
      end
    end

    def channel(value)
      name, weight = value.to_s.split(",", 2)
      weight ? [name, Integer(weight)] : name
    end
  end
end
