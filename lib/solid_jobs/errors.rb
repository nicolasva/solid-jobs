# frozen_string_literal: true

module SolidJobs
  class Error < StandardError; end
  class InvalidJobError < ArgumentError; end
  class InvalidArgumentError < InvalidJobError; end
  class ConfigurationError < Error; end
  class ExecutionHalt < Error; end
end
