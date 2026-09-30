# frozen_string_literal: true

require "json"
require "set"

module SolidJobs
  class IntegrityCheck
    ACTIVE_SETS = {
      "schedule" => "SCHEDULED",
      "retry" => "RETRY",
      "dead" => "DEAD",
    }.freeze

    Report = Struct.new(
      :lost,
      :orphaned,
      :invalid_reservations,
      :dangling_indexes,
      :duplicates,
      :malformed_payloads,
      :states,
      keyword_init: true,
    ) do
      def ok?
        lost.empty? &&
          orphaned.empty? &&
          invalid_reservations.empty? &&
          dangling_indexes.empty? &&
          duplicates.empty? &&
          malformed_payloads.empty?
      end
    end

    def self.call(expected_job_ids:, acked_job_ids: [], acked_key: nil, config: SolidJobs.config)
      new(
        expected_job_ids: expected_job_ids,
        acked_job_ids: acked_job_ids,
        acked_key: acked_key,
        config: config,
      ).call
    end

    def initialize(expected_job_ids:, acked_job_ids:, acked_key:, config:)
      @expected = expected_job_ids.map(&:to_s).to_set
      @acked = acked_job_ids.map(&:to_s).to_set
      @acked.merge(Array(config.redis_pool.call("SMEMBERS", acked_key))) if acked_key
      @redis = config.redis_pool
      @states = Hash.new { |hash, job_id| hash[job_id] = [] }
      @malformed_payloads = []
      @invalid_reservations = []
    end

    def call
      read_queues
      read_sorted_sets
      reserved = read_reservations
      validate_reservation_metadata(reserved)
      @acked.each { |job_id| add_state(job_id, "ACKED", "acknowledged") }

      observed = @states.keys.to_set
      Report.new(
        lost: (@expected - observed).sort,
        orphaned: (observed - @expected).sort,
        invalid_reservations: @invalid_reservations,
        dangling_indexes: dangling_attempts,
        duplicates: duplicates,
        malformed_payloads: @malformed_payloads,
        states: @states.to_h,
      )
    end

    private

    def read_queues
      scan("queue:*").each do |key|
        Array(@redis.call("LRANGE", key, 0, -1)).each_with_index do |raw, index|
          add_payload(raw, "READY", "#{key}[#{index}]")
        end
      end
    end

    def read_sorted_sets
      ACTIVE_SETS.each do |key, state|
        Array(@redis.call("ZRANGE", key, 0, -1)).each_with_index do |raw, index|
          add_payload(raw, state, "#{key}[#{index}]")
        end
      end
    end

    def read_reservations
      scan("*:reserved:*").to_h do |key|
        records = Array(@redis.call("LRANGE", key, 0, -1)).each_with_index.map do |raw, index|
          payload = parse_payload(raw, "#{key}[#{index}]")
          add_state(payload["jid"], "RESERVED", "#{key}[#{index}]") if payload
          payload
        end.compact
        [key, records]
      end
    end

    def validate_reservation_metadata(reserved)
      metadata = reservation_metadata
      reserved.each do |key, payloads|
        identity, worker_id = key.split(":reserved:", 2)
        entry = metadata.delete([identity, worker_id])
        if payloads.empty?
          @invalid_reservations << issue(key, "empty_reserved_list")
        elsif payloads.length > 1
          @invalid_reservations << issue(key, "multiple_payloads_for_worker")
        end
        payload = payloads.first
        unless entry
          @invalid_reservations << issue(key, "missing_metadata", job_id: payload&.[]("jid"))
          next
        end
        next unless payload

        attributes = entry.fetch(:attributes)
        {
          "job_id" => payload["jid"],
          "process_id" => identity,
          "worker_id" => worker_id,
          "queue" => payload["queue"],
        }.each do |field, expected|
          next if attributes[field].to_s == expected.to_s

          @invalid_reservations << issue(
            key,
            "metadata_mismatch",
            field: field,
            expected: expected,
            actual: attributes[field],
          )
        end
      end
      metadata.each_value do |entry|
        @invalid_reservations << issue(
          entry.fetch(:key),
          "metadata_without_payload",
          field: entry.fetch(:field),
          job_id: entry.fetch(:attributes)["job_id"],
        )
      end
    end

    def reservation_metadata
      scan("*:reservations").each_with_object({}) do |key, entries|
        identity = key.delete_suffix(":reservations")
        hash_entries(key).each do |field, raw|
          attributes = parse_json(raw, "#{key}[#{field}]")
          next unless attributes

          entries[[identity, field]] = {
            key: key,
            field: field,
            attributes: attributes,
          }
        end
      end
    end

    def dangling_attempts
      hash_entries("solid-jobs:attempts").filter_map do |job_id, attempt|
        next if @states[job_id]&.any? { |location| location.fetch(:state) != "ACKED" }

        {job_id: job_id, attempt: Integer(attempt), index: "solid-jobs:attempts"}
      end
    end

    def duplicates
      @states.filter_map do |job_id, locations|
        next unless locations.length > 1

        {job_id: job_id, locations: locations}
      end
    end

    def add_payload(raw, state, location)
      payload = parse_payload(raw, location)
      add_state(payload["jid"], state, location) if payload
    end

    def parse_payload(raw, location)
      payload = parse_json(raw, location)
      return unless payload
      return payload if payload["jid"]

      @malformed_payloads << {location: location, error: "missing jid", raw: raw}
      nil
    end

    def parse_json(raw, location)
      JSON.parse(raw)
    rescue JSON::ParserError => error
      @malformed_payloads << {location: location, error: error.message, raw: raw}
      nil
    end

    def add_state(job_id, state, location)
      @states[job_id.to_s] << {state: state, location: location}
    end

    def hash_entries(key)
      Array(@redis.call("HGETALL", key)).each_slice(2).to_h
    end

    def scan(pattern)
      cursor = "0"
      keys = []
      loop do
        cursor, batch = @redis.call("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
        keys.concat(batch)
        break if cursor == "0"
      end
      keys
    end

    def issue(key, reason, **details)
      {key: key, reason: reason}.merge(details)
    end
  end
end
