# frozen_string_literal: true

module SolidJobs
  module Backends
    class NotSupported < Error; end

    # Contract shared by every storage/execution backend. SolidJobs owns the
    # job API; adapters translate these operations to their engine.
    #
    # A "claim" is whatever #claim returns (an opaque reservation holder that
    # exposes #id, #channel, #envelope and #attempt). It carries the
    # reservation token / fencing token, so #ack and #retry_job take the
    # claim instead of a bare job id.
    #
    # Delivery is at-least-once on every backend. Differences in guarantees
    # are exposed through #capabilities and #supports?; operations a backend
    # cannot honor raise NotSupported instead of silently degrading.
    class Base
      CAPABILITIES = %i[
        scheduling fencing_tokens lease_extension get_job cancel deduplication
        crash_recovery server_side_retries
      ].freeze

      # Persists one job. Returns { id:, duplicate: }.
      def enqueue(_envelope, **_options) = raise(NotImplementedError)

      # Persists a job that becomes ready at `at` (Unix seconds).
      def schedule(_envelope, at:, **_options) = raise(NotImplementedError)

      def enqueue_many(envelopes)
        envelopes.each { |envelope| envelope.key?("run_at") ? schedule(envelope, at: envelope["run_at"]) : enqueue(envelope) }
        []
      end

      # Reserves the next ready job of `channel`, or returns nil.
      def claim(_channel) = raise(NotImplementedError)

      def ack(_claim) = raise(NotImplementedError)

      # Re-queues a claimed job after `delay` seconds (nil: backend policy).
      def retry_job(_claim, delay: nil) = raise(NotImplementedError)

      # Moves a claimed job to the dead state (no further retries).
      def fail_job(_claim) = raise(NotSupported, "#{self.class.name} cannot fail jobs natively")

      def get_job(_job_id) = raise(NotSupported, "#{self.class.name} cannot look up jobs by id")

      # Reclaims jobs owned by dead workers; returns how many were recovered.
      def recover(**_options) = 0

      def capabilities = []

      def supports?(capability) = capabilities.include?(capability)

      def close = self
    end
  end
end
