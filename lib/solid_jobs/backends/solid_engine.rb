# frozen_string_literal: true

require "json"
require "securerandom"
require "socket"
require "uri"
require "zlib"

module SolidJobs
  module Backends
    # Native client and backend adapter for the SolidEngine storage engine
    # (binary protocol v1 over Unix sockets or TCP).
    #
    #   SolidJobs.config.backend = :solid_engine
    #   SolidJobs.config.url = "unix:///var/run/solidengine.sock"
    class SolidEngine < Base
      MAGIC = 0x474E4553
      VERSION = 1
      HEADER_LEN = 20
      RESPONSE_BIT = 0x80
      ERROR = 0xFF
      DELAY_FROM_POLICY = 0xFFFF_FFFF_FFFF_FFFF

      PING = 1
      PUSH_JOB = 2
      CLAIM_NEXT_JOB = 3
      ACK_JOB = 4
      NACK_JOB = 5
      EXTEND_LEASE = 6
      GET_JOB = 7
      CANCEL_JOB = 8
      WAIT_FOR_JOB = 9
      GET_QUEUE_STATS = 10

      ERROR_CODES = {
        1 => :bad_request, 2 => :not_found, 3 => :stale_reservation, 4 => :invalid_state,
        5 => :overloaded, 6 => :payload_too_large, 7 => :internal, 8 => :shutting_down,
      }.freeze
      JOB_STATES = %i[ready claimed completed scheduled dead cancelled].freeze
      STATS_FIELDS = %i[
        ready claimed scheduled completed dead cancelled pushed claims acks nacks lease_expirations
      ].freeze

      class Error < StandardError
        attr_reader :code

        def initialize(code, message)
          @code = code
          super("#{code}: #{message}")
        end
      end

      class ProtocolError < StandardError; end

      CAPABILITIES = %i[
        scheduling fencing_tokens lease_extension get_job cancel deduplication
        crash_recovery server_side_retries
      ].freeze

      Lease = Struct.new(:job_id, :reservation_id, :fencing_token, :lease_deadline, :attempts, :payload, :envelope,
                         keyword_init: true) do
        def id = envelope["id"]
        def channel = envelope["channel"]
        def attempt = attempts
      end

      attr_reader :url, :lease_ms

      def initialize(url:, channel_ids: {}, lease_ms: 30_000, timeout: 5.0)
        super()
        @url = URI(url)
        @channel_ids = channel_ids.transform_keys(&:to_s)
        @lease_ms = Integer(lease_ms)
        @timeout = timeout
        @mutex = Mutex.new
        @socket = nil
        @request_id = 0
      end

      def ping
        call(PING)
        true
      end

      def enqueue(envelope, run_at: nil, priority: 0, max_attempts: 0, partition_key: "")
        envelope = envelope.transform_keys(&:to_s)
        id = (envelope["id"] ||= SecureRandom.uuid)
        run_at ||= envelope.delete("run_at")
        delay_ms = run_at ? [((run_at.to_f - Time.now.to_f) * 1000).ceil, 0].max : 0
        body = pack_u128(uuid_to_int(id))
        body << [channel_id(envelope.fetch("channel", "default")), priority, max_attempts, delay_ms].pack("S<CL<Q<")
        body << pack_bytes(partition_key.to_s) << pack_bytes(JSON.generate(envelope))
        _type, reply = call(PUSH_JOB, body)
        { id: id, duplicate: reply.getbyte(16) != 0 }
      end

      def schedule(envelope, at: nil, run_at: nil, **options)
        enqueue(envelope, run_at: at || run_at, **options)
      end

      def enqueue_many(envelopes)
        envelopes.each do |envelope|
          failures = envelope["max_failures"]
          bounded = failures && !envelope["retry_within"]
          enqueue(envelope, max_attempts: bounded ? Integer(failures) + 1 : 0)
        end
        []
      end

      def capabilities = CAPABILITIES

      def claim(channel = "default", lease_ms: @lease_ms, wait_ms: 0)
        body = [channel_id(channel), lease_ms, wait_ms].pack("S<L<L<")
        _type, reply = call(CLAIM_NEXT_JOB, body, read_timeout: (wait_ms / 1000.0) + @timeout)
        return nil if reply.getbyte(0) == 0

        job_id = unpack_u128(reply, 1)
        reservation_id, fencing, deadline, attempts, length = reply.byteslice(17, 32).unpack("Q<Q<Q<L<L<")
        payload = reply.byteslice(49, length)
        Lease.new(job_id: job_id, reservation_id: reservation_id, fencing_token: fencing,
                  lease_deadline: deadline, attempts: attempts, payload: payload,
                  envelope: JSON.parse(payload))
      end

      def ack(lease)
        call(ACK_JOB, reservation(lease))
        true
      end

      # Retries the job; delay: nil applies the engine's backoff policy.
      def nack(lease, delay: nil, fail: false)
        flags = fail ? 1 : 0
        delay_ms = if fail then 0 elsif delay.nil? then DELAY_FROM_POLICY else (delay * 1000).ceil end
        call(NACK_JOB, reservation(lease) + [flags, delay_ms].pack("CQ<"))
        true
      end

      def fail_job(lease)
        nack(lease, fail: true)
      end

      def retry_job(lease, delay: nil)
        nack(lease, delay: delay)
      end

      # The engine reclaims expired leases itself; nothing to do client-side.
      def recover(**) = 0

      def extend_lease(lease, lease_ms: @lease_ms)
        _type, reply = call(EXTEND_LEASE, reservation(lease) + [lease_ms].pack("L<"))
        lease.lease_deadline = reply.unpack1("Q<")
      end

      def cancel(job_id)
        call(CANCEL_JOB, pack_u128(uuid_to_int(job_id)))
        true
      end

      def get_job(job_id) = job(job_id)

      def job(job_id)
        _type, reply = call(GET_JOB, pack_u128(uuid_to_int(job_id)))
        channel, state, priority, attempts, max_attempts, fencing, deadline, scheduled_at, created_at, length =
          reply.byteslice(16, reply.bytesize - 16).unpack("S<CCL<L<Q<Q<Q<Q<L<")
        { channel_id: channel, state: JOB_STATES.fetch(state), priority: priority, attempts: attempts,
          max_attempts: max_attempts, fencing_token: fencing, lease_deadline: deadline,
          scheduled_at: scheduled_at, created_at: created_at, payload_length: length }
      end

      def stats(channel: nil)
        body = channel ? [1, channel_id(channel)].pack("CS<") : [0, 0].pack("CS<")
        _type, reply = call(GET_QUEUE_STATS, body)
        STATS_FIELDS.zip(reply.unpack("Q<11")).to_h
      end

      def close
        @mutex.synchronize do
          @socket&.close
          @socket = nil
        end
      end

      def channel_id(name)
        @channel_ids.fetch(name.to_s) { (Zlib.crc32(name.to_s) % 65_535) + 1 }
      end

      def uuid_to_int(id)
        hex = id.to_s.delete("-")
        raise ArgumentError, "job id must be a UUID: #{id.inspect}" unless hex.match?(/\A\h{32}\z/)

        hex.to_i(16)
      end

      def int_to_uuid(value)
        hex = value.to_s(16).rjust(32, "0")
        "#{hex[0, 8]}-#{hex[8, 4]}-#{hex[12, 4]}-#{hex[16, 4]}-#{hex[20, 12]}"
      end

      private

      def reservation(lease)
        pack_u128(lease.job_id) + [lease.reservation_id, lease.fencing_token].pack("Q<Q<")
      end

      def pack_u128(value)
        [value & 0xFFFF_FFFF_FFFF_FFFF, value >> 64].pack("Q<Q<")
      end

      def unpack_u128(data, offset)
        low, high = data.byteslice(offset, 16).unpack("Q<Q<")
        (high << 64) | low
      end

      def pack_bytes(string)
        string = string.b
        [string.bytesize].pack("L<") + string
      end

      def call(type, body = "".b, read_timeout: @timeout)
        @mutex.synchronize do
          attempts = 0
          begin
            socket = (@socket ||= connect)
            @request_id += 1
            write_frame(socket, type, @request_id, body.b)
            read_response(socket, type, @request_id, read_timeout)
          rescue Errno::EPIPE, Errno::ECONNRESET, EOFError, IOError
            @socket&.close
            @socket = nil
            attempts += 1
            # Only idempotent reads are replayed after a lost connection.
            retry if attempts < 2 && [PING, GET_JOB, GET_QUEUE_STATS].include?(type)
            raise
          end
        end
      end

      def connect
        case @url.scheme
        when "unix" then UNIXSocket.new(@url.path)
        when "tcp" then TCPSocket.new(@url.host, @url.port).tap { |s| s.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1) }
        else raise ArgumentError, "unsupported SolidEngine url scheme: #{@url.scheme.inspect}"
        end
      end

      def write_frame(socket, type, request_id, body)
        header = [MAGIC, VERSION, type, 0, request_id, body.bytesize].pack("L<CCS<Q<L<")
        crc = Zlib.crc32(body, Zlib.crc32(header))
        socket.write(header + body + [crc].pack("L<"))
      end

      def read_response(socket, request_type, request_id, timeout)
        header = read_exact(socket, HEADER_LEN, timeout)
        magic, version, type, _flags, id, length = header.unpack("L<CCS<Q<L<")
        raise ProtocolError, "bad magic" unless magic == MAGIC
        raise ProtocolError, "unsupported version #{version}" unless version == VERSION

        body = read_exact(socket, length, timeout)
        crc = read_exact(socket, 4, timeout).unpack1("L<")
        raise ProtocolError, "checksum mismatch" unless Zlib.crc32(body, Zlib.crc32(header)) == crc
        raise ProtocolError, "unexpected request id #{id}" unless id == request_id

        if type == ERROR
          code = ERROR_CODES.fetch(body.unpack1("S<"), :internal)
          raise Error.new(code, body.byteslice(6, body.byteslice(2, 4).unpack1("L<")))
        end
        raise ProtocolError, "unexpected response type #{type}" unless type == (request_type | RESPONSE_BIT)

        [type, body]
      end

      def read_exact(socket, count, timeout)
        data = "".b
        while data.bytesize < count
          unless socket.wait_readable(timeout)
            @socket&.close
            @socket = nil
            raise ProtocolError, "read timeout"
          end
          chunk = socket.read_nonblock(count - data.bytesize, exception: false)
          raise EOFError if chunk.nil?
          data << chunk unless chunk == :wait_readable
        end
        data
      end
    end
  end
end
