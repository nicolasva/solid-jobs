# frozen_string_literal: true

# Standalone reproducer for the Ruby 3.4 Ractor GC-barrier deadlock.
# It does not use SolidJobs or Redis: one receiver Ractor plus four senders
# exchanging `move: true` messages is enough to freeze the whole VM
# (threads park in rb_ractor_sched_barrier_join) on Ruby 3.4.x, while
# Ruby 4.0.x completes every iteration.
#
#   ruby test/support/ractor_barrier_repro.rb secondary 30
#   ruby test/support/ractor_barrier_repro.rb main 30
#
# "secondary" receives in a secondary thread (SolidJobs' heartbeat layout),
# "main" receives in the Ractor's main thread; both deadlock on 3.4.

$stdout.sync = true
require "timeout"

mode = ARGV.fetch(0, "secondary")
iterations = Integer(ARGV.fetch(1, "30"))
hangs = 0

iterations.times do
  receiver = Ractor.new(mode) do |layout|
    queue = ::Queue.new
    stopping = false
    drain = lambda do
      until queue.empty?
        stopping = true if queue.pop(true) == :stop
      end
    end
    if layout == "secondary"
      listener = Thread.new do
        loop do
          message = Ractor.receive
          queue << message
          break if message == :stop
        end
      end
      until stopping
        drain.call
        sleep 0.005 unless stopping
      end
      listener.join
    else
      worker = Thread.new do
        until stopping
          drain.call
          sleep 0.005 unless stopping
        end
      end
      loop do
        message = Ractor.receive
        queue << message
        break if message == :stop
      end
      worker.join
    end
    :stopped
  end
  senders = Array.new(4) do |id|
    Ractor.new(receiver, id) do |target, sender_id|
      3000.times do |n|
        target.send([:work, sender_id.to_s, {"n" => n}], move: true)
        target.send([:done, sender_id.to_s], move: true)
      end
      :ok
    end
  end
  senders.each { |sender| sender.respond_to?(:value) ? sender.value : sender.take }
  receiver.send(:stop)
  begin
    Timeout.timeout(5) { receiver.respond_to?(:value) ? receiver.value : receiver.take }
    print "."
  rescue Timeout::Error
    hangs += 1
    print "H"
  end
end

puts "\n#{RUBY_VERSION} #{mode}: #{hangs}/#{iterations} hangs"
exit!(0)
