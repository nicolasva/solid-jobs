# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../support/redis_test_server"
require "open3"
require "rbconfig"

class RespReaderStartupTest < Minitest::Test
  WORKER = File.expand_path("../support/resp_reader_startup_worker.rb", __dir__)

  def test_simultaneous_reader_startup_is_native_crash_free
    run_cycles("simultaneous", 8, "reader")
    run_cycles("simultaneous", 16, "reader")
    run_cycles("simultaneous", 8, "client")
    run_cycles("simultaneous", 16, "client")
  end

  def test_serialized_reader_startup_is_native_crash_free
    run_cycles("serialized", 8, "reader")
    run_cycles("serialized", 16, "reader")
    run_cycles("serialized", 8, "client")
    run_cycles("serialized", 16, "client")
  end

  private

  def run_cycles(mode, ractors, transport)
    cycles = Integer(ENV.fetch("STARTUP_TORTURE_CYCLES", "5"))
    readers = Integer(ENV.fetch("STARTUP_TORTURE_READERS", "100"))
    cycles.times do |cycle|
      stdout, stderr, status = Open3.capture3(
        RbConfig.ruby,
        WORKER,
        ractors.to_s,
        readers.to_s,
        mode,
        ENV.fetch("STARTUP_TORTURE_WARM_MAIN", "true"),
        transport,
        RedisTestServer.config.server_url,
      )
      assert status.success?, <<~MESSAGE
        RESP Reader startup failed: mode=#{mode}, transport=#{transport}, ractors=#{ractors},
        cycle=#{cycle}, status=#{status.inspect}
        stdout:
        #{stdout}
        stderr:
        #{stderr}
      MESSAGE
    end
  end
end
