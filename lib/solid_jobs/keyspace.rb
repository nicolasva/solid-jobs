# frozen_string_literal: true

module SolidJobs
  module Keyspace
    PREFIX = "solid_jobs".freeze
    CHANNELS = "#{PREFIX}:channels".freeze
    PAUSED_CHANNELS = "#{PREFIX}:paused_channels".freeze
    PLANNED = "#{PREFIX}:planned".freeze
    RETRIES = "#{PREFIX}:retries".freeze
    DISCARDED = "#{PREFIX}:discarded".freeze
    NODES = "#{PREFIX}:nodes".freeze
    ATTEMPTS = "#{PREFIX}:attempts".freeze
    PROCESSED = "#{PREFIX}:metrics:processed".freeze
    FAILED = "#{PREFIX}:metrics:failed".freeze

    module_function

    def channel(name)
      "#{PREFIX}:channel:#{name}"
    end

    def node(identity)
      "#{PREFIX}:node:#{identity}"
    end

    def node_work(identity)
      "#{node(identity)}:work"
    end

    def node_signals(identity)
      "#{node(identity)}:signals"
    end

    def claims(identity)
      "#{node(identity)}:claims"
    end

    def claimed(identity, executor_id)
      "#{node(identity)}:claimed:#{executor_id}"
    end
  end
end
